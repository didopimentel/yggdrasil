package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"google.golang.org/grpc"
)

func main() {
	// --- Init control-plane orchestrator ---
	controlPlane := controlplane.New()

	// --- gRPC server ---
	grpcServer := grpc.NewServer()

	// Register API implementations (server-side)
	pb.RegisterControlServiceServer(grpcServer, controlPlane)

	// --- Start listening ---
	addr := ":9000" // MVP, can be flag/env configurable
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	log.Printf("[yggplane-cp] listening on %s", addr)

	// Run server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server exited: %v", err)
		}
	}()

	// --- Graceful shutdown ---
	waitForShutdown(func() {
		log.Printf("[yggplane-cp] shutting down gracefully...")
		grpcServer.GracefulStop()
	})
}

// waitForShutdown handles ctrl+c / SIGINT / SIGTERM cleanly.
func waitForShutdown(onShutdown func()) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	s := <-sigCh
	log.Printf("[yggplane-cp] received signal: %v", s)
	onShutdown()
}
