package repository

import (
	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/internal/entities"
)

type ServerRegistryRepository struct {
	*ristretto.Cache[entities.ServerID, entities.Server]
}

func NewServerRegistryRepository(cache *ristretto.Cache[entities.ServerID, entities.Server]) *ServerRegistryRepository {
	return &ServerRegistryRepository{Cache: cache}
}

func (r *ServerRegistryRepository) GetServer(serverID entities.ServerID) (entities.Server, bool) {
	value, found := r.Cache.Get(serverID)
	return value, found
}

func (r *ServerRegistryRepository) SetServer(serverID entities.ServerID, server entities.Server) bool {
	return r.Cache.Set(serverID, server, 1)
}

func (r *ServerRegistryRepository) DeleteServer(serverID entities.ServerID) {
	r.Cache.Del(serverID)
}
