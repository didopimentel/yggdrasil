FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o controlplane ./cmd/controlplane

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/controlplane /controlplane
EXPOSE 9000
ENV YGGDRASIL_ADDR=:9000
ENTRYPOINT ["/controlplane"]
