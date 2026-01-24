# Makefile for generating Go protobuf and gRPC code

PROTOC ?= protoc
PROTO_DIR := api/proto
OUT_DIR := api/pb
# Collect .proto files from root and one-level subfolders (Windows-friendly)
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto) $(wildcard $(PROTO_DIR)/*/*.proto)

# Determine GOBIN for plugin installs; fallback to GOPATH/bin
GOBIN ?= $(shell go env GOBIN)
ifeq ($(strip $(GOBIN)),)
  GOBIN := $(shell go env GOPATH)/bin
endif

.PHONY: proto deps

proto: $(PROTO_FILES)
	@mkdir -p $(OUT_DIR)
	$(PROTOC) --proto_path=$(PROTO_DIR) \
	  --go_out=$(OUT_DIR) --go_opt=paths=source_relative \
	  --go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
	  $(PROTO_FILES)

# Install required protoc plugins
deps:
	@echo "Installing protoc plugins to $(GOBIN)"
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Ensure $(GOBIN) is on your PATH."