package entities

type ServerID string

type Server struct {
	ID      ServerID
	Address string
	Port    uint32
}
