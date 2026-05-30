package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"github.com/didopimentel/yggdrasil/internal/dashboard"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// --- Read and validate configuration from environment ---
	cellAmount := uint64(500)
	if v := os.Getenv("YGGDRASIL_CELL_AMOUNT"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			logger.Error("invalid YGGDRASIL_CELL_AMOUNT", "value", v, "error", err)
			os.Exit(1)
		}
		if parsed == 0 {
			logger.Error("YGGDRASIL_CELL_AMOUNT must be > 0")
			os.Exit(1)
		}
		cellAmount = parsed
	}

	addr := ":9000"
	if v := os.Getenv("YGGDRASIL_ADDR"); v != "" {
		addr = v
	}
	if addr == "" {
		logger.Error("YGGDRASIL_ADDR must be non-empty")
		os.Exit(1)
	}

	dashboardAddr := ":9001"
	if v := os.Getenv("YGGDRASIL_DASHBOARD_ADDR"); v != "" {
		dashboardAddr = v
	}

	serverToCellRatio := 10
	if v := os.Getenv("YGGDRASIL_SERVER_TO_CELL_RATIO"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			logger.Error("invalid YGGDRASIL_SERVER_TO_CELL_RATIO", "value", v, "error", err)
			os.Exit(1)
		}
		if parsed <= 0 {
			logger.Error("YGGDRASIL_SERVER_TO_CELL_RATIO must be > 0")
			os.Exit(1)
		}
		serverToCellRatio = parsed
	}

	// --- Initialize repositories ---
	cellRegistryRepository := repository.NewCellRegistryRepository(cellAmount)
	playerPositionRepository := repository.NewPlayerPositionRepository()
	playerServerRepository := repository.NewPlayerServerRepository()
	serverRegistryRepository := repository.NewServerRegistryRepository()

	// --- Define grid ---
	grid := entities.Grid{
		Width:     int(cellAmount),
		Height:    1,
		CellSizeX: 1,
		CellSizeY: 1,
		OriginX:   0,
		OriginY:   0,
	}

	// --- Dashboard state tracker ---
	tracker := dashboard.NewTracker()

	// --- Init control-plane orchestrator ---
	controlPlane := controlplane.New(logger)
	placementManager := placement.New(placement.Params{
		Logger:             logger,
		CellRegistry:       cellRegistryRepository,
		PlayerPositionRepo: playerPositionRepository,
		PlayerServerRepo:   playerServerRepository,
		ServerRegistry:     serverRegistryRepository,
		Grid:               grid,
		MigrationNotifier:  controlPlane,
		Observer:           tracker,
	})
	serverManager := servermanager.New(servermanager.Params{
		Logger:            logger,
		ServerToCellRatio: serverToCellRatio,
		CellRegistry:      cellRegistryRepository,
		ServerRegistry:    serverRegistryRepository,
		Observer:          tracker,
	})

	// --- gRPC server ---
	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, controlPlane)
	pb.RegisterPlacementServiceServer(grpcServer, placementManager)
	pb.RegisterServerManagerServiceServer(grpcServer, serverManager)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	logger.Info("controlplane server starting", "grpc", addr, "dashboard", dashboardAddr)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server exited", "error", err)
			os.Exit(1)
		}
	}()

	// --- Dashboard HTTP server ---
	httpServer := &http.Server{
		Addr:    dashboardAddr,
		Handler: dashboard.NewHandler(tracker),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("dashboard HTTP server exited", "error", err)
			os.Exit(1)
		}
	}()

	// --- Graceful shutdown ---
	waitForShutdown(func() {
		logger.Info("shutting down gracefully")
		grpcServer.GracefulStop()
		httpServer.Close()
	})
}

func waitForShutdown(onShutdown func()) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	s := <-sigCh
	slog.Info("received shutdown signal", "signal", s)
	onShutdown()
}
