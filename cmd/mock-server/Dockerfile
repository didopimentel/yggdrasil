FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o mock-server ./cmd/mock-server

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/mock-server /mock-server
ENTRYPOINT ["/mock-server"]
