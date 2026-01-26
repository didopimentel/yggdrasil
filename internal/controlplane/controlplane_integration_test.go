package controlplane_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestOpenControlStreamConnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, controlplane.New(slog.Default()))

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
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := pb.NewControlServiceClient(conn)
	stream, err := client.OpenControlStream(ctx, &pb.OpenControlStreamRequest{ServerId: &pb.ServerId{Id: "test-server"}})
	if err != nil {
		t.Fatalf("OpenControlStream failed: %v", err)
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("expected stream to close cleanly, got: %v", err)
	}
}
