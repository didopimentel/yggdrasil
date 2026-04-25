package controlplane

import (
	"context"
	"log/slog"
	"sync"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverStream struct {
	events chan *pb.ControlEvent
	cancel context.CancelFunc
}

type ControlPlane struct {
	pb.UnimplementedControlServiceServer
	logger  *slog.Logger
	mu      sync.RWMutex
	streams map[entities.ServerID]*serverStream
}

func New(logger *slog.Logger) *ControlPlane {
	return &ControlPlane{
		logger:  logger,
		streams: make(map[entities.ServerID]*serverStream),
	}
}

func (cp *ControlPlane) OpenControlStream(req *pb.OpenControlStreamRequest, stream grpc.ServerStreamingServer[pb.ControlEvent]) error {
	if req.ServerId == nil || req.ServerId.Id == "" {
		cp.logger.Warn("control stream opened with no server ID")
		return status.Error(codes.InvalidArgument, "server ID is required")
	}

	serverID := entities.ServerID(req.ServerId.Id)
	cp.logger.Info("control stream opened", "server_id", serverID)

	ctx, cancel := context.WithCancel(stream.Context())
	ss := &serverStream{
		events: make(chan *pb.ControlEvent, 64),
		cancel: cancel,
	}

	cp.mu.Lock()
	cp.streams[serverID] = ss
	cp.mu.Unlock()

	go func() {
		for event := range ss.events {
			if err := stream.Send(event); err != nil {
				cp.logger.Warn("failed to send control event", "server_id", serverID, "error", err)
				cancel()
				return
			}
		}
	}()

	<-ctx.Done()

	cp.mu.Lock()
	close(ss.events)
	delete(cp.streams, serverID)
	cp.mu.Unlock()

	return nil
}

func (cp *ControlPlane) NotifyMigration(ctx context.Context, playerID entities.PlayerID, oldServerID entities.ServerID, newServer entities.Server) error {
	cp.mu.RLock()
	ss, ok := cp.streams[oldServerID]
	cp.mu.RUnlock()

	if !ok {
		return nil
	}

	event := &pb.ControlEvent{
		Type: pb.ControlEventType_CONTROL_EVENT_MIGRATE_OUT,
		Payload: &pb.ControlEvent_MigrateOut{
			MigrateOut: &pb.MigrateOutEvent{
				PlayerId: &pb.PlayerId{Id: string(playerID)},
				NewServer: &pb.Server{
					Id:      string(newServer.ID),
					Address: newServer.Address,
					Port:    newServer.Port,
				},
			},
		},
	}

	select {
	case ss.events <- event:
	default:
	}

	return nil
}
