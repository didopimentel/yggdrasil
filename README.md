# Yggdrasil

A control plane for massively-multiplayer game servers. Yggdrasil partitions the game world into a grid of cells, assigns each cell to a game server, and routes players to the correct server based on their world position. It handles player migration when players cross cell boundaries and manages the full server lifecycle over persistent gRPC streams.

## Concepts

| Concept | Description |
|---|---|
| **Cell** | Atomic unit of the game world. The world is divided into N cells (default: 500). |
| **Server** | A game server instance. Claims ownership of a fixed number of cells on registration. |
| **Placement** | Routing a player to the game server that owns the cell at their position. |
| **Migration** | Moving a player from one server to another when they cross a cell boundary. |
| **Control stream** | A persistent server-streaming connection from a game server to the control plane, used to push events (e.g. `MIGRATE_OUT`) down to the server. |

## Architecture

```
game server          control plane (Yggdrasil)
    │                        │
    ├──OpenControlStream────►│  server-streaming: receives ControlEvents
    │                        │
    ├──RegisterServer───────►│  bidi-streaming: claims cells on startup
    │                        │
    ├──AssignPlayer─────────►│  bidi-streaming: place a player on connect
    ├──UpdatePlayerPosition─►│  bidi-streaming: track position changes
    ├──CompleteMigration────►│  bidi-streaming: ack after cross-server hop
    │                        │
    └──UnregisterServer─────►│  bidi-streaming: release cells on shutdown
```

All communication is bidirectional gRPC streaming. The control plane replies with a `ControlAck{ok, message}` on every inbound message, and pushes `ControlEvent` messages (such as `MIGRATE_OUT`) down the control stream when it decides a player should change servers.

## gRPC API

Proto package: `yggplane.v1` — sources under `api/proto/`.

### ControlService

```protobuf
service ControlService {
  // Game server opens this stream; control plane pushes events down it.
  rpc OpenControlStream(OpenControlStreamRequest) returns (stream ControlEvent);
}
```

`ControlEvent` carries a `ControlEventType` and a typed payload (currently `MigrateOutEvent`).

### PlacementService

```protobuf
service PlacementService {
  rpc AssignPlayer(stream AssignPlayerRequest)             returns (stream ControlAck);
  rpc UpdatePlayerPosition(stream UpdatePlayerPositionRequest) returns (stream ControlAck);
  rpc CompleteMigration(stream CompleteMigrationRequest)  returns (stream ControlAck);
}
```

### ServerManagerService

```protobuf
service ServerManagerService {
  rpc RegisterServer(stream RegisterServerRequest)     returns (stream ControlAck);
  rpc UnregisterServer(stream UnregisterServerRequest) returns (stream ControlAck);
}
```

## Project layout

```
cmd/
  controlplane/       main entry point — wires dependencies, starts gRPC server
api/
  proto/              .proto source files
  pb/                 generated Go code (do not edit)
internal/
  entities/           domain types: Grid, Cell, Server, Player, Position
  repository/         in-memory stores (Ristretto) for cells, players, servers
  controlplane/       ControlService implementation
  placement/          PlacementService implementation
  servermanager/      ServerManagerService implementation — cell assignment logic
```

## Running

```sh
go run ./cmd/controlplane
```

Listens on `:9000` (TCP). Structured JSON logs go to stdout.

Shutdown on `SIGINT` or `SIGTERM` drains in-flight gRPC calls before exiting.

## Building

```sh
go build -trimpath -ldflags "-X main.version=$TAG" ./cmd/controlplane
```

## Regenerating protobuf

```sh
buf generate
```

Requires [buf](https://buf.build) and the `protoc-gen-go` / `protoc-gen-go-grpc` plugins.

## Testing

```sh
go test -race ./...
```

Integration tests live alongside their packages (`*_integration_test.go`).

## Dependencies

| Package | Purpose |
|---|---|
| `google.golang.org/grpc` | gRPC transport |
| `google.golang.org/protobuf` | Protocol Buffers runtime |
| `github.com/dgraph-io/ristretto/v2` | High-throughput in-memory cache for cell/player/server state |
