package repository

import (
	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/internal/entities"
)

type PlayerPositionRepository struct {
	*ristretto.Cache[entities.PlayerID, entities.Position]
}

func NewPlayerPositionRepository(cache *ristretto.Cache[entities.PlayerID, entities.Position]) *PlayerPositionRepository {
	return &PlayerPositionRepository{cache}
}

func (r *PlayerPositionRepository) GetPlayerPosition(playerID entities.PlayerID) (entities.Position, bool) {
	value, found := r.Cache.Get(playerID)
	if !found {
		return entities.Position{}, false
	}

	return value, true
}

func (r *PlayerPositionRepository) SetPlayerPosition(playerID entities.PlayerID, position entities.Position) bool {
	return r.Cache.Set(playerID, position, 1)
}

func (r *PlayerPositionRepository) DeletePlayerPosition(playerID entities.PlayerID) {
	r.Cache.Del(playerID)
}
