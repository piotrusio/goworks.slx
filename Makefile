# Makefile for building the SLX cross-platform service

# --- Variables ---
# Define output directories and binary names
BUILD_DIR=./bin
WINDOWS_BINARY=$(BUILD_DIR)/slx-windows.exe
UNIX_BINARY=$(BUILD_DIR)/slx-unix

# Define paths to the main packages
WINDOWS_MAIN=./cmd/slx-windows
UNIX_MAIN=./cmd/slx-unix

# --- Build Commands ---

# Default command: build both binaries
.PHONY: all
all: build

# Build both binaries by calling their specific targets
.PHONY: build
build: build-windows build-unix

# Builds the Windows executable
.PHONY: build-windows
build-windows:
	@echo "--> Building Windows executable..."
	@mkdir -p $(BUILD_DIR)
	@GOOS=windows GOARCH=amd64 go build -o $(WINDOWS_BINARY) $(WINDOWS_MAIN)
	@echo "--> Build complete: $(WINDOWS_BINARY)"

# Builds the Unix executable
.PHONY: build-unix
build-unix:
	@echo "--> Building Unix executable..."
	@mkdir -p $(BUILD_DIR)
	@GOOS=linux GOARCH=amd64 go build -o $(UNIX_BINARY) $(UNIX_MAIN)
	@echo "--> Build complete: $(UNIX_BINARY)"

# --- Formatting ---

# Formats Go code and organizes imports using goimports
.PHONY: fmt
fmt:
	@echo "--> Formatting Go code..."
	@goimports -w .
	@echo "--> Formatting complete."

# --- Cleanup ---

# Removes all build artifacts
.PHONY: clean
clean:
	@echo "--> Cleaning up build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "--> Cleanup complete."