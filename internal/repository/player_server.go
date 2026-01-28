package repository

import (
	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/internal/entities"
)

type PlayerServerRepository struct {
	*ristretto.Cache[entities.PlayerID, entities.ServerID]
}

func NewPlayerServerRepository(cache *ristretto.Cache[entities.PlayerID, entities.ServerID]) *PlayerServerRepository {
	return &PlayerServerRepository{Cache: cache}
}

func (r *PlayerServerRepository) GetPlayerServer(playerID entities.PlayerID) (entities.ServerID, bool) {
	value, found := r.Cache.Get(playerID)
	return value, found
}

func (r *PlayerServerRepository) SetPlayerServer(playerID entities.PlayerID, serverID entities.ServerID) bool {
	return r.Cache.Set(playerID, serverID, 1)
}

func (r *PlayerServerRepository) DeletePlayerServer(playerID entities.PlayerID) {
	r.Cache.Del(playerID)
}
