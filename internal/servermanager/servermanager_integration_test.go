package servermanager_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRegisterServer_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	cellOwnerCache, err := ristretto.NewCache(&ristretto.Config[entities.Cell, entities.ServerID]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create cell owner cache: %v", err)
	}
	t.Cleanup(cellOwnerCache.Close)

	serverCellsCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, []entities.Cell]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create server cells cache: %v", err)
	}
	t.Cleanup(serverCellsCache.Close)

	serverRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, entities.Server]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create server registry cache: %v", err)
	}
	t.Cleanup(serverRegistryCache.Close)

	cellRegistry := repository.NewCellRegistryRepository(cellOwnerCache, serverCellsCache, 100)
	serverRegistry := repository.NewServerRegistryRepository(serverRegistryCache)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterServerManagerServiceServer(grpcServer, servermanager.New(servermanager.Params{
		Logger:            slog.Default(),
		ServerToCellRatio: 10,
		CellRegistry:      cellRegistry,
		ServerRegistry:    serverRegistry,
	}))

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

	client := pb.NewServerManagerServiceClient(conn)
	stream, err := client.RegisterServer(ctx)
	if err != nil {
		t.Fatalf("RegisterServer failed: %v", err)
	}

	if err := stream.Send(&pb.RegisterServerRequest{Server: &pb.Server{Id: "s1", Address: "addr"}}); err != nil {
		t.Fatalf("send request: %v", err)
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

	// Verify that the server was assigned to the first 10 cells
	serverID := entities.ServerID("s1")
	assignedCells, found := serverCellsCache.Get(serverID)
	if !found {
		t.Fatalf("expected server to be assigned to cells, but no entry found in cache")
	}
	if len(assignedCells) != 10 {
		t.Fatalf("expected server to be assigned to 10 cells, got %d", len(assignedCells))
	}

	// Verify the cells are the first 10 (0-9)
	for i, cell := range assignedCells {
		if cell != entities.Cell(i) {
			t.Fatalf("expected cell at index %d to be %d, got %d", i, i, cell)
		}
	}
}

func TestRegisterServer_ValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	cellOwnerCache, err := ristretto.NewCache(&ristretto.Config[entities.Cell, entities.ServerID]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create cell owner cache: %v", err)
	}
	t.Cleanup(cellOwnerCache.Close)

	serverCellsCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, []entities.Cell]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create server cells cache: %v", err)
	}
	t.Cleanup(serverCellsCache.Close)

	serverRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, entities.Server]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("failed to create server registry cache: %v", err)
	}
	t.Cleanup(serverRegistryCache.Close)

	cellRegistry := repository.NewCellRegistryRepository(cellOwnerCache, serverCellsCache, 100)
	serverRegistry := repository.NewServerRegistryRepository(serverRegistryCache)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterServerManagerServiceServer(grpcServer, servermanager.New(servermanager.Params{
		Logger:            slog.Default(),
		ServerToCellRatio: 10,
		CellRegistry:      cellRegistry,
		ServerRegistry:    serverRegistry,
	}))

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

	client := pb.NewServerManagerServiceClient(conn)
	stream, err := client.RegisterServer(ctx)
	if err != nil {
		t.Fatalf("RegisterServer failed: %v", err)
	}

	if err := stream.Send(&pb.RegisterServerRequest{Server: &pb.Server{Address: "addr"}}); err != nil {
		t.Fatalf("send request: %v", err)
	}

	ack, recvErr := stream.Recv()
	if recvErr != nil {
		t.Fatalf("recv ack: %v", recvErr)
	}
	if ack.GetOk() {
		t.Fatalf("expected validation failure, got ok=true")
	}
	if ack.GetMessage() != "server.id is required" {
		t.Fatalf("expected server.id validation message, got: %q", ack.GetMessage())
	}
}
