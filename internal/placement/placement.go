package placement

import (
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
)

type PlacementManager struct {
	logger *slog.Logger
}

func New(logger *slog.Logger) *PlacementManager {
	return &PlacementManager{logger: logger}
}

func (p *PlacementManager) AssignPlayer(player *pb.PlayerId, pos *pb.Position) error {
	p.logger.Info("assign player", "player_id", player.GetId(), "position", pos)
	return nil
}

func (p *PlacementManager) UpdatePlayerPosition(player *pb.PlayerId, pos *pb.Position) error {
	p.logger.Info("update player position", "player_id", player.GetId(), "position", pos)
	return nil
}

func (p *PlacementManager) CompleteMigration(player *pb.PlayerId, serverID *pb.ServerId) error {
	p.logger.Info("complete migration", "player_id", player.GetId(), "server_id", serverID.GetId())
	return nil
}
