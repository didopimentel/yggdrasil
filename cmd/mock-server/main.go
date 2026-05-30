package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/didopimentel/yggdrasil/api/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	serverID := os.Getenv("MOCK_SERVER_ID")
	if serverID == "" {
		hostname, _ := os.Hostname()
		serverID = hostname
	}

	serverAddr := os.Getenv("MOCK_SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = serverID
	}

	serverPort := uint32(8000)
	if v := os.Getenv("MOCK_SERVER_PORT"); v != "" {
		p, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			logger.Error("invalid MOCK_SERVER_PORT", "value", v, "error", err)
			os.Exit(1)
		}
		serverPort = uint32(p)
	}

	cpAddr := os.Getenv("YGGDRASIL_ADDR")
	if cpAddr == "" {
		cpAddr = "localhost:9000"
	}

	cellAmount := int64(500)
	if v := os.Getenv("YGGDRASIL_CELL_AMOUNT"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			logger.Error("invalid YGGDRASIL_CELL_AMOUNT", "value", v, "error", err)
			os.Exit(1)
		}
		cellAmount = parsed
	}

	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to create grpc client", "addr", cpAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	smClient := pb.NewServerManagerServiceClient(conn)
	placementClient := pb.NewPlacementServiceClient(conn)
	controlClient := pb.NewControlServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// RegisterServer is bidi-stream: send one request, recv one ack, close.
	if err := registerWithRetry(ctx, logger, smClient, serverID, serverAddr, serverPort); err != nil {
		logger.Error("failed to register server", "error", err)
		os.Exit(1)
	}

	// Open long-lived bidi streams for placement operations.
	assignStream, err := placementClient.AssignPlayer(ctx)
	if err != nil {
		logger.Error("open assign stream failed", "error", err)
		os.Exit(1)
	}
	updateStream, err := placementClient.UpdatePlayerPosition(ctx)
	if err != nil {
		logger.Error("open update stream failed", "error", err)
		os.Exit(1)
	}
	migrateStream, err := placementClient.CompleteMigration(ctx)
	if err != nil {
		logger.Error("open migrate stream failed", "error", err)
		os.Exit(1)
	}
	controlStream, err := controlClient.OpenControlStream(ctx, &pb.OpenControlStreamRequest{
		ServerId: &pb.ServerId{Id: serverID},
	})
	if err != nil {
		logger.Error("open control stream failed", "error", err)
		os.Exit(1)
	}

	logger.Info("streams open", "serverID", serverID)

	// Drain acks — keeps streams healthy and logs nacks.
	go drainAcks(ctx, logger, "assign", func() (*pb.ControlAck, error) { return assignStream.Recv() })
	go drainAcks(ctx, logger, "update", func() (*pb.ControlAck, error) { return updateStream.Recv() })
	go drainAcks(ctx, logger, "migrate", func() (*pb.ControlAck, error) { return migrateStream.Recv() })

	// Per-stream mutexes — gRPC streams are not concurrent-send-safe.
	var (
		assignMu  sync.Mutex
		updateMu  sync.Mutex
		migrateMu sync.Mutex
	)

	var (
		playersMu sync.Mutex
		players   = make(map[string]*pb.Position)
		nextIdx   int
	)

	// Handle migration events pushed from control plane.
	go func() {
		for {
			event, err := controlStream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("control stream recv error", "error", err)
				return
			}
			if event.GetType() != pb.ControlEventType_CONTROL_EVENT_MIGRATE_OUT {
				continue
			}
			m := event.GetMigrateOut()
			if m == nil {
				continue
			}
			playerID := m.GetPlayerId().GetId()
			newServerID := m.GetNewServer().GetId()
			logger.Info("migrate out", "player", playerID, "newServer", newServerID)

			migrateMu.Lock()
			sendErr := migrateStream.Send(&pb.CompleteMigrationRequest{
				PlayerId: &pb.PlayerId{Id: playerID},
				ServerId: &pb.ServerId{Id: newServerID},
			})
			migrateMu.Unlock()
			if sendErr != nil {
				logger.Error("complete migration send failed", "player", playerID, "error", sendErr)
				continue
			}

			playersMu.Lock()
			delete(players, playerID)
			playersMu.Unlock()
		}
	}()

	// Simulation loop: assign new players and move existing ones every 500ms.
	ticker := time.NewTicker(5000 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// ~30% chance: spawn a new player at a random position.
				if rand.Float32() < 0.3 {
					playersMu.Lock()
					nextIdx++
					playerID := fmt.Sprintf("%s-p%d", serverID, nextIdx)
					playersMu.Unlock()

					pos := &pb.Position{X: rand.Float64() * float64(cellAmount)}

					assignMu.Lock()
					sendErr := assignStream.Send(&pb.AssignPlayerRequest{
						PlayerId: &pb.PlayerId{Id: playerID},
						Position: pos,
					})
					assignMu.Unlock()
					if sendErr != nil {
						logger.Error("assign player send failed", "player", playerID, "error", sendErr)
						continue
					}

					playersMu.Lock()
					players[playerID] = pos
					playersMu.Unlock()
					logger.Info("player assigned", "player", playerID, "x", pos.X)
				}

				// Move all known players by ±1 unit.
				playersMu.Lock()
				snapshot := make(map[string]*pb.Position, len(players))
				maps.Copy(snapshot, players)
				playersMu.Unlock()

				for playerID, pos := range snapshot {
					newPos := &pb.Position{
						X: clamp(pos.X+(rand.Float64()*2-1), 0, float64(cellAmount-1)),
					}

					updateMu.Lock()
					sendErr := updateStream.Send(&pb.UpdatePlayerPositionRequest{
						PlayerId: &pb.PlayerId{Id: playerID},
						Position: newPos,
					})
					updateMu.Unlock()
					if sendErr != nil {
						logger.Error("update position send failed", "player", playerID, "error", sendErr)
						continue
					}

					playersMu.Lock()
					players[playerID] = newPos
					playersMu.Unlock()
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	s := <-sigCh
	logger.Info("shutting down", "signal", s)
	cancel()

	assignStream.CloseSend()
	updateStream.CloseSend()
	migrateStream.CloseSend()

	unregister(logger, smClient, serverID)
}

// registerWithRetry opens a RegisterServer bidi stream, sends one request, waits for ack.
// Retries with exponential backoff — controlplane may still be starting.
func registerWithRetry(ctx context.Context, logger *slog.Logger, client pb.ServerManagerServiceClient, id, addr string, port uint32) error {
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= 10; attempt++ {
		err := func() error {
			stream, err := client.RegisterServer(ctx)
			if err != nil {
				return err
			}
			defer stream.CloseSend()
			if err = stream.Send(&pb.RegisterServerRequest{
				Server: &pb.Server{Id: id, Address: addr, Port: port},
			}); err != nil {
				return err
			}
			ack, err := stream.Recv()
			if err != nil {
				return err
			}
			if !ack.GetOk() {
				return fmt.Errorf("nack: %s", ack.GetMessage())
			}
			return nil
		}()
		if err == nil {
			logger.Info("registered", "serverID", id, "attempt", attempt)
			return nil
		}
		logger.Warn("register attempt failed, retrying", "attempt", attempt, "backoff", backoff, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("exhausted retries registering %s", id)
}

// unregister opens an UnregisterServer bidi stream, sends one request, waits for ack.
func unregister(logger *slog.Logger, client pb.ServerManagerServiceClient, id string) {
	stream, err := client.UnregisterServer(context.Background())
	if err != nil {
		logger.Error("unregister stream open failed", "error", err)
		return
	}
	defer stream.CloseSend()
	if err = stream.Send(&pb.UnregisterServerRequest{
		ServerId: &pb.ServerId{Id: id},
	}); err != nil {
		logger.Error("unregister send failed", "error", err)
		return
	}
	ack, err := stream.Recv()
	if err != nil {
		logger.Error("unregister recv failed", "error", err)
		return
	}
	if !ack.GetOk() {
		logger.Warn("unregister nack", "message", ack.GetMessage())
		return
	}
	logger.Info("unregistered", "serverID", id)
}

func drainAcks(ctx context.Context, logger *slog.Logger, stream string, recv func() (*pb.ControlAck, error)) {
	for {
		ack, err := recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("stream recv error", "stream", stream, "error", err)
			return
		}
		if !ack.GetOk() {
			logger.Warn("nack received", "stream", stream, "message", ack.GetMessage())
		}
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
