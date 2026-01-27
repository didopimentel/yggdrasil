package controlplane

import (
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControlPlane struct {
	pb.UnimplementedControlServiceServer
	logger           *slog.Logger
	serverManager    *servermanager.ServerManager
	placementManager *placement.PlacementManager
}

func New(logger *slog.Logger) *ControlPlane {
	return &ControlPlane{
		logger:           logger,
		serverManager:    servermanager.New(logger),
		placementManager: placement.New(logger),
	}
}

func (cp *ControlPlane) OpenControlStream(req *pb.OpenControlStreamRequest, stream grpc.ServerStreamingServer[pb.ControlEvent]) error {
	if req.ServerId == nil {
		cp.logger.Warn("control stream opened with no server ID")
		return status.Error(codes.InvalidArgument, "server ID is required")
	}

	cp.logger.Info("control stream opened", "server_id", req.ServerId.Id)

	return nil
}
