# ==============================================================================
# GMX Historical Data - Makefile Configuration
# ==============================================================================
# You can override these defaults:
#   1. Edit values below
#   2. Pass on command line: make funding-unified INCLUDE_DATASTORE=1
# ==============================================================================

# Network configuration
NETWORK ?= arbitrum

# Directory paths
UNIFIED_OUTPUT_DIR ?= ./data/funding
FEATHER_DIR ?= ./user_data
LOG_DIR ?= ./logs
CHECKPOINT_DIR ?= ./checkpoints

# Extraction options
OUTPUT_FORMAT ?= parquet
FROM_BLOCK ?=
TO_BLOCK ?=
MARKET ?=

# Set INCLUDE_DATASTORE=1 to include archive RPC reads (slow, requires JSON_RPC_ARBITRUM)
INCLUDE_DATASTORE ?=

# ==============================================================================
# Targets
# ==============================================================================

.PHONY: help install show-config \
        funding-unified funding-unified-resume funding-unified-merge \
        funding-feather funding-full \
        funding-status funding-stop funding-logs funding-clean

# Default target
help:
	@echo "GMX Historical Data Makefile"
	@echo ""
	@echo "Configuration (current values):"
	@echo "  NETWORK             = $(NETWORK)"
	@echo "  UNIFIED_OUTPUT_DIR  = $(UNIFIED_OUTPUT_DIR)"
	@echo "  FEATHER_DIR         = $(FEATHER_DIR)"
	@echo "  LOG_DIR             = $(LOG_DIR)"
	@echo "  OUTPUT_FORMAT       = $(OUTPUT_FORMAT)"
	@echo "  INCLUDE_DATASTORE   = $(if $(INCLUDE_DATASTORE),yes (archive RPC required),no)"
	@echo ""
	@echo "Main targets:"
	@echo "  funding-unified      - Extract all phases + merge (HyperSync only, fast)"
	@echo "  funding-full         - Extract all phases including DataStore (slow, archive RPC)"
	@echo "  funding-feather      - Export to FreqTrade feather format (run after extraction)"
	@echo "  funding-unified-resume - Incremental update (resume from checkpoints)"
	@echo "  funding-unified-merge  - Merge only (no re-extraction)"
	@echo ""
	@echo "Utility targets:"
	@echo "  install              - Install dependencies with Poetry"
	@echo "  funding-clean        - Clean up temporary files"
	@echo "  show-config          - Show all configuration variables"
	@echo ""
	@echo "Usage examples:"
	@echo "  make funding-unified                           # Fast: HyperSync phases only"
	@echo "  make funding-full                              # Full history including DataStore"
	@echo "  make funding-feather                           # Export to feather after extraction"
	@echo "  make funding-full MARKET=ETH/USD               # Single market"
	@echo "  make funding-unified-resume                    # Top up stale markets"
	@echo ""
	@echo "Tip: set JSON_RPC_ARBITRUM before funding-full"
	@echo "  export JSON_RPC_ARBITRUM=https://your-archive-node"

# Show current configuration
show-config:
	@echo "Current Configuration:"
	@echo "  NETWORK             = $(NETWORK)"
	@echo "  UNIFIED_OUTPUT_DIR  = $(UNIFIED_OUTPUT_DIR)"
	@echo "  FEATHER_DIR         = $(FEATHER_DIR)"
	@echo "  CHECKPOINT_DIR      = $(CHECKPOINT_DIR)"
	@echo "  LOG_DIR             = $(LOG_DIR)"
	@echo "  OUTPUT_FORMAT       = $(OUTPUT_FORMAT)"
	@echo "  INCLUDE_DATASTORE   = $(if $(INCLUDE_DATASTORE),yes,no)"
	@echo "  FROM_BLOCK          = $(FROM_BLOCK)"
	@echo "  TO_BLOCK            = $(TO_BLOCK)"
	@echo "  MARKET              = $(MARKET)"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	poetry install
	@echo "Done"

# ==============================================================================
# Unified Extraction (recommended entry point)
# ==============================================================================

define UNIFIED_CMD
poetry run python scripts/extract_unified_funding.py \
	--network $(NETWORK) \
	--output-dir $(UNIFIED_OUTPUT_DIR) \
	--output $(OUTPUT_FORMAT) \
	$(if $(INCLUDE_DATASTORE),--include-datastore,) \
	$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),) \
	$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
	$(if $(MARKET),--market $(MARKET),)
endef

# HyperSync phases only (fast, no archive RPC needed)
funding-unified:
	@echo "Starting unified funding rate extraction (HyperSync only)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR)
	$(UNIFIED_CMD)

# Full history including DataStore (slow, requires archive RPC)
funding-full:
	@echo "Starting full funding rate extraction (HyperSync + DataStore)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo "  RPC:        $$JSON_RPC_ARBITRUM"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR) $(CHECKPOINT_DIR)
	$(UNIFIED_CMD) --include-datastore

# Incremental update (resume from checkpoints, skip already-extracted ranges)
funding-unified-resume:
	@echo "Starting incremental unified funding rate extraction..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR)
	$(UNIFIED_CMD) --resume

# Merge only (all raw data already extracted)
funding-unified-merge:
	@echo "Merging existing funding rate data..."
	@mkdir -p $(UNIFIED_OUTPUT_DIR)
	poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir $(UNIFIED_OUTPUT_DIR) \
		--merge-only \
		$(if $(MARKET),--market $(MARKET),)

# Export to FreqTrade feather format (run after extraction)
funding-feather:
	@echo "Exporting to FreqTrade feather format..."
	@echo "  Source:     $(UNIFIED_OUTPUT_DIR)"
	@echo "  Feather:    $(FEATHER_DIR)"
	@echo ""
	@mkdir -p $(FEATHER_DIR)
	poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir $(UNIFIED_OUTPUT_DIR) \
		--output feather \
		--feather-dir $(FEATHER_DIR) \
		--merge-only \
		$(if $(MARKET),--market $(MARKET),)

# ==============================================================================
# Utility targets
# ==============================================================================

# Clean up temporary files
funding-clean:
	@echo "Cleaning up temporary files..."
	@rm -rf test_* gmx_v2_funding_*.json
	@echo "Done"
	@echo ""
	@echo "To clean persistent data, run manually:"
	@echo "  rm -rf $(UNIFIED_OUTPUT_DIR)   # Delete all extracted data"
	@echo "  rm -rf $(CHECKPOINT_DIR)       # Delete checkpoints"
	@echo "  rm -rf $(LOG_DIR)              # Delete logs"

# Status / stop / logs kept for compatibility with background processes
funding-status:
	@echo "No background process tracking in unified mode."
	@echo "Use 'ps aux | grep extract_unified' to check for running processes."

funding-stop:
	@echo "Use Ctrl+C to stop a foreground process, or 'kill' for background."

funding-logs:
	@echo "Use 'tail -f $(LOG_DIR)/funding.log' or watch terminal output directly."
