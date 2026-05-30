package repository

import (
	"sync"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

type CellRegistryRepository struct {
	mu              sync.RWMutex
	cellOwner       map[entities.Cell]entities.ServerID
	serverCells     map[entities.ServerID][]entities.Cell
	unassignedCells []entities.Cell
}

func NewCellRegistryRepository(cellAmount uint64) *CellRegistryRepository {
	unassigned := make([]entities.Cell, cellAmount)
	for i := range cellAmount {
		unassigned[i] = entities.Cell(i)
	}
	return &CellRegistryRepository{
		cellOwner:       make(map[entities.Cell]entities.ServerID),
		serverCells:     make(map[entities.ServerID][]entities.Cell),
		unassignedCells: unassigned,
	}
}

func (r *CellRegistryRepository) GetCellOwner(cell entities.Cell) (entities.ServerID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.cellOwner[cell]
	return v, ok
}

// AssignCells atomically picks up to n unassigned cells and assigns them to serverID.
func (r *CellRegistryRepository) AssignCells(serverID entities.ServerID, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > len(r.unassignedCells) {
		n = len(r.unassignedCells)
	}
	for i := range n {
		cell := r.unassignedCells[i]
		r.cellOwner[cell] = serverID
		r.serverCells[serverID] = append(r.serverCells[serverID], cell)
	}
	r.unassignedCells = r.unassignedCells[n:]
}

func (r *CellRegistryRepository) GetServerCells(serverID entities.ServerID) ([]entities.Cell, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cells, ok := r.serverCells[serverID]
	return cells, ok
}

func (r *CellRegistryRepository) UnassignServerFromAllCells(serverID entities.ServerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cells, ok := r.serverCells[serverID]
	if !ok {
		return
	}
	for _, cell := range cells {
		delete(r.cellOwner, cell)
	}
	delete(r.serverCells, serverID)
	r.unassignedCells = append(r.unassignedCells, cells...)
}
