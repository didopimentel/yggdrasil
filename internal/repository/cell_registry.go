package repository

import (
	"sync"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/internal/entities"
)

type CellRegistryRepository struct {
	CellOwner       *ristretto.Cache[entities.Cell, entities.ServerID]
	ServerCells     *ristretto.Cache[entities.ServerID, []entities.Cell]
	CellAmount      uint64
	mu              sync.Mutex
	unassignedCells []entities.Cell
}

func NewCellRegistryRepository(cellOwner *ristretto.Cache[entities.Cell, entities.ServerID], serverCells *ristretto.Cache[entities.ServerID, []entities.Cell], cellAmount uint64) *CellRegistryRepository {
	unassignedCells := make([]entities.Cell, 0, cellAmount)
	for i := range cellAmount {
		unassignedCells = append(unassignedCells, entities.Cell(i))
	}
	return &CellRegistryRepository{CellOwner: cellOwner, ServerCells: serverCells, CellAmount: cellAmount, unassignedCells: unassignedCells}
}

func (r *CellRegistryRepository) GetCellOwner(cell entities.Cell) (entities.ServerID, bool) {
	value, found := r.CellOwner.Get(cell)
	return value, found
}

// AssignCells atomically picks up to n unassigned cells and assigns them to serverID.
func (r *CellRegistryRepository) AssignCells(serverID entities.ServerID, n int) {
	r.mu.Lock()
	toAssign := n
	if len(r.unassignedCells) < toAssign {
		toAssign = len(r.unassignedCells)
	}
	cells := make([]entities.Cell, toAssign)
	copy(cells, r.unassignedCells[:toAssign])
	r.unassignedCells = r.unassignedCells[toAssign:]
	r.mu.Unlock()

	for _, cell := range cells {
		r.assignServerToCell(serverID, cell)
	}
}

func (r *CellRegistryRepository) assignServerToCell(serverID entities.ServerID, cell entities.Cell) {
	r.CellOwner.Set(cell, serverID, 1)
	r.CellOwner.Wait()
	value, found := r.ServerCells.Get(serverID)
	if found {
		value = append(value, cell)
	} else {
		value = []entities.Cell{cell}
	}
	r.ServerCells.Set(serverID, value, 1)
	r.ServerCells.Wait()
}

func (r *CellRegistryRepository) UnassignServerFromAllCells(serverID entities.ServerID) {
	cells, found := r.ServerCells.Get(serverID)
	if !found {
		return
	}

	for _, cell := range cells {
		r.CellOwner.Del(cell)
	}
	r.ServerCells.Del(serverID)

	r.mu.Lock()
	r.unassignedCells = append(r.unassignedCells, cells...)
	r.mu.Unlock()
}
