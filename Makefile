# ==============================================================================
# GMX Historical Data - Makefile Configuration
# ==============================================================================
# You can override these defaults:
#   1. Edit values below
#   2. Pass on command line: make funding-full OUTPUT_DIR=/custom/path
# ==============================================================================

# Network configuration
NETWORK ?= arbitrum

# Directory paths
OUTPUT_DIR ?= ./user_data/data/gmx
CHECKPOINT_DIR ?= ./checkpoints
LOG_DIR ?= ./logs

# File paths
LOG_FILE ?= $(LOG_DIR)/funding.log
PID_FILE ?= $(LOG_DIR)/funding.pid

# Extraction options
OUTPUT_FORMAT ?= parquet
FROM_BLOCK ?=
TO_BLOCK ?=
MARKET ?=

# Background mode
BACKGROUND ?= --background

# ==============================================================================
# Targets
# ==============================================================================

.PHONY: help install funding-full funding-incremental funding-foreground funding-status funding-stop funding-logs funding-clean show-config

# Default target
help:
	@echo "GMX Historical Data Makefile"
	@echo ""
	@echo "Configuration (current values):"
	@echo "  NETWORK         = $(NETWORK)"
	@echo "  OUTPUT_DIR      = $(OUTPUT_DIR)"
	@echo "  CHECKPOINT_DIR  = $(CHECKPOINT_DIR)"
	@echo "  LOG_DIR         = $(LOG_DIR)"
	@echo "  OUTPUT_FORMAT   = $(OUTPUT_FORMAT)"
	@echo ""
	@echo "Available targets:"
	@echo "  install              - Install dependencies with Poetry"
	@echo "  funding-full         - Run full historical extraction (background)"
	@echo "  funding-incremental  - Run incremental extraction (background)"
	@echo "  funding-foreground   - Run extraction in foreground (for debugging)"
	@echo "  funding-status       - Check status of background extraction"
	@echo "  funding-stop         - Stop background extraction"
	@echo "  funding-logs         - Tail extraction logs in real-time"
	@echo "  funding-clean        - Clean up temporary files"
	@echo "  show-config          - Show all configuration variables"
	@echo ""
	@echo "Usage Examples:"
	@echo "  make install                              # First time setup"
	@echo "  make funding-full                         # Start extraction with defaults"
	@echo "  make funding-full OUTPUT_DIR=/mnt/data    # Custom output directory"
	@echo "  make funding-status                       # Check if running"
	@echo "  make funding-logs                         # Watch progress"
	@echo ""
	@echo "Override defaults on command line:"
	@echo "  make funding-full NETWORK=avalanche OUTPUT_DIR=/custom/path"
	@echo ""
	@echo "Or edit defaults at the top of this Makefile"

# Show current configuration
show-config:
	@echo "Current Configuration:"
	@echo "  NETWORK         = $(NETWORK)"
	@echo "  OUTPUT_DIR      = $(OUTPUT_DIR)"
	@echo "  CHECKPOINT_DIR  = $(CHECKPOINT_DIR)"
	@echo "  LOG_DIR         = $(LOG_DIR)"
	@echo "  LOG_FILE        = $(LOG_FILE)"
	@echo "  PID_FILE        = $(PID_FILE)"
	@echo "  OUTPUT_FORMAT   = $(OUTPUT_FORMAT)"
	@echo "  FROM_BLOCK      = $(FROM_BLOCK)"
	@echo "  TO_BLOCK        = $(TO_BLOCK)"
	@echo "  MARKET          = $(MARKET)"
	@echo "  BACKGROUND      = $(BACKGROUND)"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	poetry install
	@echo "✓ Installation complete"

# Build the extraction command with all options
define EXTRACT_CMD
poetry run python scripts/extract_funding_rates.py \
	--network $(NETWORK) \
	--output $(OUTPUT_FORMAT) \
	--output-dir $(OUTPUT_DIR) \
	--resume \
	--checkpoint-dir $(CHECKPOINT_DIR) \
	$(if $(BACKGROUND),$(BACKGROUND),) \
	$(if $(BACKGROUND),--log-file $(LOG_FILE),) \
	$(if $(BACKGROUND),--pid-file $(PID_FILE),) \
	$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),) \
	$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
	$(if $(MARKET),--market $(MARKET),)
endef

# Full historical extraction (background mode)
funding-full:
	@echo "Starting full historical funding rate extraction..."
	@echo "  Network:     $(NETWORK)"
	@echo "  Output dir:  $(OUTPUT_DIR)"
	@echo "  Checkpoints: $(CHECKPOINT_DIR)"
	@echo "  Log file:    $(LOG_FILE)"
	@echo "  Mode:        background"
	@echo ""
	@mkdir -p $(OUTPUT_DIR) $(CHECKPOINT_DIR) $(LOG_DIR)
	@$(EXTRACT_CMD)
	@echo ""
	@echo "✓ Background extraction started"
	@echo "  Check status: make funding-status"
	@echo "  Watch logs:   make funding-logs"

# Incremental extraction (for daily/hourly updates)
funding-incremental:
	@echo "Starting incremental funding rate extraction..."
	@echo "  Network:     $(NETWORK)"
	@echo "  Output dir:  $(OUTPUT_DIR)"
	@echo "  Checkpoints: $(CHECKPOINT_DIR)"
	@echo "  Log file:    $(LOG_FILE)"
	@echo "  Mode:        incremental + background"
	@echo ""
	@mkdir -p $(OUTPUT_DIR) $(CHECKPOINT_DIR) $(LOG_DIR)
	@$(EXTRACT_CMD)
	@echo ""
	@echo "✓ Background extraction started"
	@echo "  Check status: make funding-status"
	@echo "  Watch logs:   make funding-logs"

# Run in foreground (for debugging)
funding-foreground:
	@echo "Starting funding rate extraction in foreground..."
	@echo "  Network:     $(NETWORK)"
	@echo "  Output dir:  $(OUTPUT_DIR)"
	@echo "  Checkpoints: $(CHECKPOINT_DIR)"
	@echo "  Mode:        foreground (Ctrl+C to stop)"
	@echo ""
	@mkdir -p $(OUTPUT_DIR) $(CHECKPOINT_DIR) $(LOG_DIR)
	@poetry run python scripts/extract_funding_rates.py \
		--network $(NETWORK) \
		--output $(OUTPUT_FORMAT) \
		--output-dir $(OUTPUT_DIR) \
		--resume \
		--checkpoint-dir $(CHECKPOINT_DIR) \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

# Check status of background process
funding-status:
	@echo "Checking funding extraction status..."
	@echo "  PID file: $(PID_FILE)"
	@echo "  Log file: $(LOG_FILE)"
	@echo ""
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if ps -p $$PID > /dev/null 2>&1; then \
			echo "✓ Funding extraction is running (PID: $$PID)"; \
			echo ""; \
			echo "Memory usage:"; \
			ps -p $$PID -o pid,vsz,rss,comm | head -2; \
			echo ""; \
			echo "Command line:"; \
			ps -p $$PID -o args= | fold -s -w 80; \
			echo ""; \
			echo "Latest log entries (last 5 lines):"; \
			tail -5 $(LOG_FILE) 2>/dev/null || echo "No logs yet"; \
		else \
			echo "✗ Funding extraction is NOT running (stale PID file)"; \
			echo ""; \
			echo "Last log entries (last 10 lines):"; \
			tail -10 $(LOG_FILE) 2>/dev/null || echo "No logs"; \
		fi \
	else \
		echo "✗ No PID file found at $(PID_FILE)"; \
		echo ""; \
		echo "Extraction is not running in background mode."; \
	fi

# Stop background extraction
funding-stop:
	@echo "Stopping funding extraction..."
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if ps -p $$PID > /dev/null 2>&1; then \
			kill $$PID && echo "✓ Sent SIGTERM to process $$PID"; \
			echo "  Waiting for graceful shutdown..."; \
			sleep 3; \
			if ps -p $$PID > /dev/null 2>&1; then \
				echo "⚠ Process still running, sending SIGKILL..."; \
				kill -9 $$PID; \
				sleep 1; \
			fi; \
			if ! ps -p $$PID > /dev/null 2>&1; then \
				echo "✓ Process stopped successfully"; \
			fi; \
			rm $(PID_FILE); \
		else \
			echo "✗ Process not running (stale PID file)"; \
			rm $(PID_FILE); \
		fi \
	else \
		echo "✗ No PID file found at $(PID_FILE)"; \
	fi

# Tail logs in real-time
funding-logs:
	@echo "Tailing funding extraction logs (Ctrl+C to exit)..."
	@echo "  Log file: $(LOG_FILE)"
	@echo ""
	@tail -f $(LOG_FILE) 2>/dev/null || echo "No log file found at $(LOG_FILE)"

# Clean up temporary files
funding-clean:
	@echo "Cleaning up temporary files..."
	@rm -rf test_* gmx_v2_funding_*.json
	@echo "✓ Cleaned up temporary test files and JSON outputs"
	@echo ""
	@echo "To clean persistent data, run manually:"
	@echo "  rm -rf $(OUTPUT_DIR)      # Delete all extracted data"
	@echo "  rm -rf $(CHECKPOINT_DIR)  # Delete checkpoints (forces full re-extraction)"
	@echo "  rm -rf $(LOG_DIR)         # Delete logs"
