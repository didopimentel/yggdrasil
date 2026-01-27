package servermanager_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRegisterServer_AckOk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterServerManagerServiceServer(grpcServer, servermanager.New(slog.Default()))

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
}

func TestRegisterServer_ValidationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterServerManagerServiceServer(grpcServer, servermanager.New(slog.Default()))

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
