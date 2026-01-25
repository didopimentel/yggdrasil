package controlplane

import (
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"google.golang.org/grpc"
)

type ControlPlane struct {
	pb.UnimplementedControlServiceServer
	logger *slog.Logger
}

func New(logger *slog.Logger) *ControlPlane {
	return &ControlPlane{
		logger: logger,
	}
}

func (cp *ControlPlane) OpenControlStream(req *pb.OpenControlStreamRequest, stream grpc.ServerStreamingServer[pb.ControlEvent]) error {
	if req.ServerId == nil {
		cp.logger.Warn("control stream opened with no server ID")
		return grpc.ErrServerStopped.Error()
	}

	if req.ServerId != nil {
		cp.logger.Info("control stream opened", "server_id", req.ServerId.Id)
	}
	return nil
}
