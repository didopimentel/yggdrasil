package entities

type Grid struct {
	OriginX, OriginY     float64
	CellSizeX, CellSizeY float64
	Width, Height        int // number of cells along X and Y
}

type Cell uint32
