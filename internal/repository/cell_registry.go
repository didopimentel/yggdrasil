package repository

import (
	"github.com/dgraph-io/ristretto/v2"
	"github.com/didopimentel/yggdrasil/internal/entities"
)

type CellRegistryRepository struct {
	CellOwner       *ristretto.Cache[entities.Cell, entities.ServerID]
	ServerCells     *ristretto.Cache[entities.ServerID, []entities.Cell]
	CellAmount      uint64
	UnassignedCells []entities.Cell
}

func NewCellRegistryRepository(cellOwner *ristretto.Cache[entities.Cell, entities.ServerID], serverCells *ristretto.Cache[entities.ServerID, []entities.Cell], cellAmount uint64) *CellRegistryRepository {
	unassignedCells := make([]entities.Cell, 0, cellAmount)
	for i := range cellAmount {
		unassignedCells = append(unassignedCells, entities.Cell(i))
	}
	return &CellRegistryRepository{CellOwner: cellOwner, ServerCells: serverCells, CellAmount: cellAmount, UnassignedCells: unassignedCells}
}

func (r *CellRegistryRepository) GetCellOwner(cell entities.Cell) (entities.ServerID, bool) {
	value, found := r.CellOwner.Get(cell)
	return value, found
}

func (r *CellRegistryRepository) AssignServerToCell(serverID entities.ServerID, cell entities.Cell) bool {
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
	r.UnassignedCells = r.removeCell(r.UnassignedCells, cell)
	return r.CellOwner.Set(cell, serverID, 1)
}

func (r *CellRegistryRepository) removeCell(cells []entities.Cell, targetCell entities.Cell) []entities.Cell {
	for i, cell := range cells {
		if cell == targetCell {
			return append(cells[:i], cells[i+1:]...)
		}
	}
	return cells
}

func (r *CellRegistryRepository) UnassignServerFromAllCells(serverID entities.ServerID) {
	cells, found := r.ServerCells.Get(serverID)
	if !found {
		return
	}

	for _, cell := range cells {
		r.CellOwner.Del(cell)
		r.UnassignedCells = append(r.UnassignedCells, cell)
	}
	r.ServerCells.Del(serverID)
}
