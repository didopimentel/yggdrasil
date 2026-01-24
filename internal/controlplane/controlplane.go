package controlplane

import (
	"github.com/didopimentel/yggdrasil/api/pb"
	"google.golang.org/grpc"
)

type ControlPlane struct {
	pb.UnimplementedControlServiceServer
}

func New() *ControlPlane {
	return &ControlPlane{}
}

func (cp *ControlPlane) OpenControlStream(*pb.OpenControlStreamRequest, grpc.ServerStreamingServer[pb.ControlEvent]) error {
	return nil
}
