package placement_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"github.com/dgraph-io/ristretto/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newTestPlacementManager(t *testing.T) *placement.PlacementManager {
	t.Helper()

	playerPositionCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.Position]{
		NumCounters: 1e4, MaxCost: 1 << 20, BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("player position cache: %v", err)
	}

	playerServerCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.ServerID]{
		NumCounters: 1e4, MaxCost: 1 << 20, BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("player server cache: %v", err)
	}

	serverRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, entities.Server]{
		NumCounters: 1e4, MaxCost: 1 << 20, BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("server registry cache: %v", err)
	}

	cellOwnerCache, err := ristretto.NewCache(&ristretto.Config[entities.Cell, entities.ServerID]{
		NumCounters: 1e4, MaxCost: 1 << 20, BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("cell owner cache: %v", err)
	}

	serverCellsCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, []entities.Cell]{
		NumCounters: 1e4, MaxCost: 1 << 20, BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("server cells cache: %v", err)
	}

	cellRegistry := repository.NewCellRegistryRepository(cellOwnerCache, serverCellsCache, 10)
	cellRegistry.AssignCells("server-1", 10)

	serverRegistry := repository.NewServerRegistryRepository(serverRegistryCache)
	serverRegistry.SetServer("server-1", entities.Server{ID: "server-1", Address: "localhost", Port: 9000})
	serverRegistry.Wait()

	grid := entities.Grid{Width: 10, Height: 1, CellSizeX: 1, CellSizeY: 1}

	return placement.New(placement.Params{
		Logger:             slog.Default(),
		CellRegistry:       cellRegistry,
		PlayerPositionRepo: repository.NewPlayerPositionRepository(playerPositionCache),
		PlayerServerRepo:   repository.NewPlayerServerRepository(playerServerCache),
		ServerRegistry:     serverRegistry,
		Grid:               grid,
		MigrationNotifier:  &noopMigrationNotifier{},
	})
}

type noopMigrationNotifier struct{}

func (n *noopMigrationNotifier) NotifyMigration(_ context.Context, _ entities.PlayerID, _ entities.ServerID, _ entities.Server) error {
	return nil
}

func TestAssignPlayer_AckOk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPlacementServiceServer(grpcServer, newTestPlacementManager(t))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := pb.NewPlacementServiceClient(conn)
	stream, err := client.AssignPlayer(ctx)
	if err != nil {
		t.Fatalf("AssignPlayer failed: %v", err)
	}

	sendErr := stream.Send(&pb.AssignPlayerRequest{
		PlayerId: &pb.PlayerId{Id: "p1"},
		Position: &pb.Position{X: 1, Y: 0, Z: 0},
	})
	if sendErr != nil {
		t.Fatalf("send request: %v", sendErr)
	}

	ack, recvErr := stream.Recv()
	if recvErr != nil {
		t.Fatalf("recv ack: %v", recvErr)
	}
	if !ack.GetOk() {
		t.Fatalf("expected ok ack, got: %+v", ack)
	}

	_ = stream.CloseSend()
	if _, err = stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF after close, got: %v", err)
	}
}

func TestAssignPlayer_ValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPlacementServiceServer(grpcServer, newTestPlacementManager(t))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := pb.NewPlacementServiceClient(conn)
	stream, err := client.AssignPlayer(ctx)
	if err != nil {
		t.Fatalf("AssignPlayer failed: %v", err)
	}

	if err := stream.Send(&pb.AssignPlayerRequest{Position: &pb.Position{X: 1}}); err != nil {
		t.Fatalf("send request: %v", err)
	}

	ack, recvErr := stream.Recv()
	if recvErr != nil {
		t.Fatalf("recv ack: %v", recvErr)
	}
	if ack.GetOk() {
		t.Fatalf("expected validation failure, got ok=true")
	}
	if ack.GetMessage() != "player_id is required" {
		t.Fatalf("expected player_id validation message, got: %q", ack.GetMessage())
	}
}
