package entities

type Grid struct {
	OriginX, OriginY     float64
	CellSizeX, CellSizeY float64
	Width, Height        int // number of cells along X and Y
}

type Cell uint32

func (g Grid) CellAt(pos Position) Cell {
	col := int((pos.X - g.OriginX) / g.CellSizeX)
	row := int((pos.Y - g.OriginY) / g.CellSizeY)

	if col < 0 {
		col = 0
	} else if col > g.Width-1 {
		col = g.Width - 1
	}

	if row < 0 {
		row = 0
	} else if row > g.Height-1 {
		row = g.Height - 1
	}

	return Cell(row*g.Width + col)
}
