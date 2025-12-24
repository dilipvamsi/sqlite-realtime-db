# Configuration
CMD_PATH=cmd/realtime/main.go
JS_SRC=clients/js/src/index.js
JS_DEST=internal/realtime/public/realtime.js

# -----------------------------------------------------------------------------
# 1. OS DETECTION & NAMING
# -----------------------------------------------------------------------------

# Detect if running on Windows (OS is a standard env var on Windows)
ifeq ($(OS),Windows_NT)
    EXT := .exe
else
    EXT := .bin
endif

# Define final binary name
BINARY_NAME := realtime-server
BINARY_NAME_WITH_EXT := $(BINARY_NAME)$(EXT)

# Go Build Flags
# -s: Omit the symbol table and debug information
# -w: Omit the DWARF symbol table
LDFLAGS=-s -w

.PHONY: all init build build-static docker-build clean run

# Default target
all: build

# -----------------------------------------------------------------------------
# 1. ASSET PREPARATION
# -----------------------------------------------------------------------------

# Ensure the public folder exists and copy the JS file
# This is a file dependency rule: if JS_SRC changes, JS_DEST is updated.
$(JS_DEST): $(JS_SRC)
	@echo "📦 Copying JS Client to embed directory..."
	@mkdir -p internal/realtime/public
	cp $(JS_SRC) $(JS_DEST)

# -----------------------------------------------------------------------------
# 2. BUILD TARGETS
# -----------------------------------------------------------------------------

# # Quick Build (Dynamic) - Best for Development
# # Uses your system's dynamic libraries. Fast build.
build-dynamic: $(JS_DEST)
	@echo "🔨 Building dynamic binary (Host OS)..."
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME)-dynamic$(EXT) $(CMD_PATH)

# Static Build (Host OS)
# Attempts to build a static binary on your current machine.
# Added tags to disable C-dependent networking and SQLite extensions.
# NOTE: On Linux, requires 'glibc-static' or 'musl-dev'.
#       On macOS, this produces a static macOS binary, NOT a Linux one.
build: $(JS_DEST)
	@echo "🔨 Building static binary (CGO enabled)..."
	CGO_ENABLED=1 go build \
		-tags "netgo osusergo sqlite_omit_load_extension" \
		-ldflags "$(LDFLAGS) -extldflags '-static'" \
		-o bin/$(BINARY_NAME_WITH_EXT) \
		$(CMD_PATH)

# -----------------------------------------------------------------------------
# 3. PORTABLE LINUX BUILD (DOCKER)
# -----------------------------------------------------------------------------

# This uses Docker to compile a fully static LINUX binary using Alpine/Musl.
# This works from macOS, Windows, or Linux and produces a portable file
# that runs on ANY Linux distro.
docker-build: $(JS_DEST)
	@echo "🐳 Building portable Linux binary via Docker..."
	docker build -t realtime-builder -f - . < Dockerfile.build
	@echo "📦 Extracting binary..."
	docker create --name temp-container realtime-builder
	docker cp temp-container:/app/$(BINARY_NAME) ./bin/$(BINARY_NAME)-linux-portable$(EXT)
	docker rm temp-container
	@echo "✅ Done! Binary is at bin/$(BINARY_NAME)-linux-portable$(EXT)"

# Helper to create the ephemeral Dockerfile needed for the rule above
Dockerfile.build:
	@echo "FROM golang:1.23-alpine" > Dockerfile.build
	@echo "RUN apk add --no-cache gcc musl-dev" >> Dockerfile.build
	@echo "WORKDIR /app" >> Dockerfile.build
	@echo "COPY go.mod go.sum ./" >> Dockerfile.build
	@echo "RUN go mod download" >> Dockerfile.build
	@echo "COPY . ." >> Dockerfile.build
	@echo "RUN CGO_ENABLED=1 GOOS=linux go build -tags \"musl netgo osusergo sqlite_omit_load_extension\" -ldflags '-s -w -extldflags \"-static\"' -o server_out $(CMD_PATH)" >> Dockerfile.build

# -----------------------------------------------------------------------------
# 4. UTILITIES
# -----------------------------------------------------------------------------

# Run the project locally
run-dev: $(JS_DEST)
	@echo "🚀 Running Dev..."
	go run ./cmd/realtime/main.go

# Run the project locally
run: build
	@echo "🚀 Running..."
	./bin/$(BINARY_NAME_WITH_EXT)

# Clean up build artifacts
clean:
	@echo "🧹 Cleaning..."
	rm -rf bin/
	rm -f $(JS_DEST)
	rm -f Dockerfile.build
