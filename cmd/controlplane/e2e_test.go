package main_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestFullPlayerJourney exercises the complete lifecycle:
//
//	server-1 registers (cells 0–9)
//	→ server-2 registers (cells 10–19)
//	→ server-1 opens control stream
//	→ player assigned at X=5 (cell 5, server-1)
//	→ player moves to X=15 (cell 15, server-2) → MIGRATE_OUT fires
//	→ server-2 completes migration
func TestFullPlayerJourney(t *testing.T) {
	t.Parallel()

	const (
		cellAmount        = uint64(20)
		serverToCellRatio = 10
	)

	cellRegistry := repository.NewCellRegistryRepository(cellAmount)
	playerPositionRepo := repository.NewPlayerPositionRepository()
	playerServerRepo := repository.NewPlayerServerRepository()
	serverRegistry := repository.NewServerRegistryRepository()

	grid := entities.Grid{Width: int(cellAmount), Height: 1, CellSizeX: 1, CellSizeY: 1}

	cp := controlplane.New(slog.Default())
	pm := placement.New(placement.Params{
		Logger:             slog.Default(),
		CellRegistry:       cellRegistry,
		PlayerPositionRepo: playerPositionRepo,
		PlayerServerRepo:   playerServerRepo,
		ServerRegistry:     serverRegistry,
		Grid:               grid,
		MigrationNotifier:  cp,
	})
	sm := servermanager.New(servermanager.Params{
		Logger:            slog.Default(),
		ServerToCellRatio: serverToCellRatio,
		CellRegistry:      cellRegistry,
		ServerRegistry:    serverRegistry,
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, cp)
	pb.RegisterPlacementServiceServer(grpcServer, pm)
	pb.RegisterServerManagerServiceServer(grpcServer, sm)

	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	smClient := pb.NewServerManagerServiceClient(conn)
	placementClient := pb.NewPlacementServiceClient(conn)
	controlClient := pb.NewControlServiceClient(conn)

	registerServer(ctx, t, smClient, "server-1", "127.0.0.1", 9001)
	registerServer(ctx, t, smClient, "server-2", "127.0.0.1", 9002)

	// server-1 opens its control stream to receive migration events.
	migrateEvents := make(chan *pb.ControlEvent, 1)
	ctrlStream, err := controlClient.OpenControlStream(ctx, &pb.OpenControlStreamRequest{
		ServerId: &pb.ServerId{Id: "server-1"},
	})
	if err != nil {
		t.Fatalf("OpenControlStream: %v", err)
	}

	time.Sleep(time.Second)

	go func() {
		for {
			event, recvErr := ctrlStream.Recv()
			if recvErr != nil {
				return
			}
			migrateEvents <- event
		}
	}()

	// Assign player p1 at X=5 → cell 5, owned by server-1.
	assignStream, err := placementClient.AssignPlayer(ctx)
	if err != nil {
		t.Fatalf("AssignPlayer stream: %v", err)
	}
	if err := assignStream.Send(&pb.AssignPlayerRequest{
		PlayerId: &pb.PlayerId{Id: "p1"},
		Position: &pb.Position{X: 5, Y: 0, Z: 0},
	}); err != nil {
		t.Fatalf("assign send: %v", err)
	}
	ack, err := assignStream.Recv()
	if err != nil {
		t.Fatalf("assign recv: %v", err)
	}
	if !ack.GetOk() {
		t.Fatalf("expected assign ok, got: %s", ack.GetMessage())
	}
	_ = assignStream.CloseSend()
	_, _ = assignStream.Recv() // drain EOF

	// Move player p1 to X=15 → cell 15, owned by server-2. Triggers MIGRATE_OUT.
	updateStream, err := placementClient.UpdatePlayerPosition(ctx)
	if err != nil {
		t.Fatalf("UpdatePlayerPosition stream: %v", err)
	}
	if err := updateStream.Send(&pb.UpdatePlayerPositionRequest{
		PlayerId: &pb.PlayerId{Id: "p1"},
		Position: &pb.Position{X: 15, Y: 0, Z: 0},
	}); err != nil {
		t.Fatalf("update send: %v", err)
	}
	updateAck, err := updateStream.Recv()
	if err != nil {
		t.Fatalf("update recv: %v", err)
	}
	if !updateAck.GetOk() {
		t.Fatalf("expected update ok, got: %s", updateAck.GetMessage())
	}
	_ = updateStream.CloseSend()
	_, _ = updateStream.Recv() // drain EOF

	// server-1 must receive a MIGRATE_OUT event for p1 pointing to server-2.
	select {
	case event := <-migrateEvents:
		if event.GetType() != pb.ControlEventType_CONTROL_EVENT_MIGRATE_OUT {
			t.Fatalf("expected MIGRATE_OUT, got %v", event.GetType())
		}
		migrateOut := event.GetMigrateOut()
		if migrateOut == nil {
			t.Fatal("MigrateOut payload is nil")
		}
		if got := migrateOut.GetPlayerId().GetId(); got != "p1" {
			t.Fatalf("expected player p1, got %q", got)
		}
		if got := migrateOut.GetNewServer().GetId(); got != "server-2" {
			t.Fatalf("expected new server server-2, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for MIGRATE_OUT event")
	}

	// server-2 confirms the player has migrated to it.
	completeMigStream, err := placementClient.CompleteMigration(ctx)
	if err != nil {
		t.Fatalf("CompleteMigration stream: %v", err)
	}
	if err := completeMigStream.Send(&pb.CompleteMigrationRequest{
		PlayerId: &pb.PlayerId{Id: "p1"},
		ServerId: &pb.ServerId{Id: "server-2"},
	}); err != nil {
		t.Fatalf("complete migration send: %v", err)
	}
	completeAck, err := completeMigStream.Recv()
	if err != nil {
		t.Fatalf("complete migration recv: %v", err)
	}
	if !completeAck.GetOk() {
		t.Fatalf("expected complete migration ok, got: %s", completeAck.GetMessage())
	}
	_ = completeMigStream.CloseSend()
	_, _ = completeMigStream.Recv() // drain EOF
}

func registerServer(ctx context.Context, t *testing.T, client pb.ServerManagerServiceClient, id, addr string, port uint32) {
	t.Helper()
	stream, err := client.RegisterServer(ctx)
	if err != nil {
		t.Fatalf("RegisterServer stream: %v", err)
	}
	if err := stream.Send(&pb.RegisterServerRequest{
		Server: &pb.Server{Id: id, Address: addr, Port: port},
	}); err != nil {
		t.Fatalf("register %s send: %v", id, err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("register %s recv: %v", id, err)
	}
	if !ack.GetOk() {
		t.Fatalf("register %s: expected ok, got: %s", id, ack.GetMessage())
	}
	_ = stream.CloseSend()
	_, _ = stream.Recv() // drain EOF
}
