package controlplane_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestOpenControlStreamConnects(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	t.Cleanup(cancel)

	client := pb.NewControlServiceClient(conn)
	stream, err := client.OpenControlStream(ctx, &pb.OpenControlStreamRequest{ServerId: &pb.ServerId{Id: "test-server"}})
	if err != nil {
		t.Fatalf("OpenControlStream failed: %v", err)
	}

	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error when context expires, got nil")
	}
	st, _ := status.FromError(recvErr)
	if st.Code().String() != "DeadlineExceeded" {
		t.Fatalf("expected DeadlineExceeded, got: %v", recvErr)
	}
}
