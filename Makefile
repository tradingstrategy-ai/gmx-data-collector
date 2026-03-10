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
DATA_DIR ?= ./data
UNIFIED_OUTPUT_DIR ?= $(DATA_DIR)/funding
OI_OUTPUT_DIR ?= ./user_data/data/gmx/open_interest
POOL_LIQUIDITY_OUTPUT_DIR ?= ./user_data/data/gmx/pool_liquidity
FEATHER_DIR ?= ./user_data
LOG_DIR ?= ./logs
CHECKPOINT_DIR ?= ./checkpoints

# Collection options (for collect targets)
CONCURRENCY ?= 5

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
        funding-status funding-stop funding-logs funding-clean \
        collect-update collect-full update-gmx-data export-freqtrade \
        oi oi-resume oi-full \
        pool-liquidity pool-liquidity-resume \
        extract-all extract-all-resume

# Default target
help:
	@echo "GMX Historical Data Makefile"
	@echo ""
	@echo "Configuration (current values):"
	@echo "  DATA_DIR                  = $(DATA_DIR)"
	@echo "  NETWORK                   = $(NETWORK)"
	@echo "  UNIFIED_OUTPUT_DIR        = $(UNIFIED_OUTPUT_DIR)"
	@echo "  OI_OUTPUT_DIR             = $(OI_OUTPUT_DIR)"
	@echo "  POOL_LIQUIDITY_OUTPUT_DIR = $(POOL_LIQUIDITY_OUTPUT_DIR)"
	@echo "  FEATHER_DIR               = $(FEATHER_DIR)"
	@echo "  LOG_DIR                   = $(LOG_DIR)"
	@echo "  CONCURRENCY               = $(CONCURRENCY)"
	@echo "  OUTPUT_FORMAT             = $(OUTPUT_FORMAT)"
	@echo "  INCLUDE_DATASTORE         = $(if $(INCLUDE_DATASTORE),yes (archive RPC required),no)"
	@echo ""
	@echo "Full collection targets (candles + funding + OI + liquidity):"
	@echo "  extract-all          - Full historical backfill: OI + pool liquidity (genesis)"
	@echo "  extract-all-resume   - Incremental update: OI + pool liquidity (resume from checkpoints)"
	@echo "  update-gmx-data      - Incremental update: candles + funding (recommended for daily runs)"
	@echo "  collect-update       - Candles only: incremental (GMX API + Chainlink + oracle)"
	@echo "  collect-full         - Candles only: full historical from genesis"
	@echo ""
	@echo "Open Interest targets:"
	@echo "  oi                   - Full historical OI extraction from genesis"
	@echo "  oi-resume            - Incremental OI update (resume from checkpoint)"
	@echo "  oi-full              - Full OI backfill with explicit from-block"
	@echo ""
	@echo "Pool Liquidity targets:"
	@echo "  pool-liquidity       - Full historical pool liquidity extraction from genesis"
	@echo "  pool-liquidity-resume - Incremental pool liquidity update (resume from checkpoint)"
	@echo ""
	@echo "Funding-only targets:"
	@echo "  funding-unified      - Extract all phases + merge (HyperSync only, fast)"
	@echo "  funding-full         - Extract all phases including DataStore (slow, archive RPC)"
	@echo "  funding-feather      - Export to FreqTrade feather format (run after extraction)"
	@echo "  funding-unified-resume - Incremental update (resume from checkpoints)"
	@echo "  funding-unified-merge  - Merge only (no re-extraction)"
	@echo ""
	@echo "Utility targets:"
	@echo "  install              - Install dependencies with Poetry"
	@echo "  export-freqtrade     - Export candles + funding to FreqTrade format"
	@echo "  funding-clean        - Clean up temporary files"
	@echo "  show-config          - Show all configuration variables"
	@echo ""
	@echo "Usage examples:"
	@echo "  source .env && make extract-all-resume        # Incremental OI + liquidity update"
	@echo "  source .env && make oi FROM_BLOCK=120000000   # Full OI backfill from genesis"
	@echo "  make pool-liquidity NETWORK=arbitrum           # Full liquidity backfill"
	@echo "  source .env && make update-gmx-data           # Candles + funding incremental"
	@echo "  make funding-unified-resume                   # Funding only, incremental"
	@echo ""
	@echo "Tip: source .env before make (for HYPERSYNC_API_TOKEN, JSON_RPC_ARBITRUM)"
	@echo "  export JSON_RPC_ARBITRUM=https://...  # Required for funding-full (DataStore phase)"

# ==============================================================================
# Open Interest Extraction
# ==============================================================================

# Full historical OI backfill from genesis
oi:
	@echo "Starting GMX V2 Open Interest extraction (full history)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(OI_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(OI_OUTPUT_DIR) $(LOG_DIR)
	poetry run python scripts/extract_open_interest.py \
		--network $(NETWORK) \
		--output-dir $(OI_OUTPUT_DIR) \
		--output parquet \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),--from-block 120000000) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

# Full OI backfill (alias for oi, kept for explicit from-block invocations)
oi-full: oi

# Incremental OI update (resume from checkpoint)
oi-resume:
	@echo "Starting incremental OI extraction (resume from checkpoint)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(OI_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(OI_OUTPUT_DIR) $(LOG_DIR)
	poetry run python scripts/extract_open_interest.py \
		--network $(NETWORK) \
		--output-dir $(OI_OUTPUT_DIR) \
		--output parquet \
		--resume \
		$(if $(MARKET),--market $(MARKET),)

# ==============================================================================
# Pool Liquidity Extraction
# ==============================================================================

# Full historical pool liquidity backfill from genesis
pool-liquidity:
	@echo "Starting GMX V2 Pool Liquidity extraction (full history)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(POOL_LIQUIDITY_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(POOL_LIQUIDITY_OUTPUT_DIR) $(LOG_DIR)
	poetry run python scripts/extract_pool_liquidity.py \
		--network $(NETWORK) \
		--output-dir $(POOL_LIQUIDITY_OUTPUT_DIR) \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),--from-block 120000000) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

# Incremental pool liquidity update (resume from checkpoint)
pool-liquidity-resume:
	@echo "Starting incremental Pool Liquidity extraction (resume from checkpoint)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(POOL_LIQUIDITY_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(POOL_LIQUIDITY_OUTPUT_DIR) $(LOG_DIR)
	poetry run python scripts/extract_pool_liquidity.py \
		--network $(NETWORK) \
		--output-dir $(POOL_LIQUIDITY_OUTPUT_DIR) \
		--resume \
		$(if $(MARKET),--market $(MARKET),)

# ==============================================================================
# Combined Extraction
# ==============================================================================

# Full historical backfill: OI + pool liquidity (runs sequentially)
extract-all: oi pool-liquidity
	@echo ""
	@echo "Full extraction complete: OI + pool liquidity"

# Incremental update: OI + pool liquidity (runs sequentially, resume from checkpoints)
extract-all-resume: oi-resume pool-liquidity-resume
	@echo ""
	@echo "Incremental extraction complete: OI + pool liquidity"

# Show current configuration
show-config:
	@echo "Current Configuration:"
	@echo "  DATA_DIR                  = $(DATA_DIR)"
	@echo "  NETWORK                   = $(NETWORK)"
	@echo "  UNIFIED_OUTPUT_DIR        = $(UNIFIED_OUTPUT_DIR)"
	@echo "  OI_OUTPUT_DIR             = $(OI_OUTPUT_DIR)"
	@echo "  POOL_LIQUIDITY_OUTPUT_DIR = $(POOL_LIQUIDITY_OUTPUT_DIR)"
	@echo "  FEATHER_DIR               = $(FEATHER_DIR)"
	@echo "  CHECKPOINT_DIR            = $(CHECKPOINT_DIR)"
	@echo "  LOG_DIR                   = $(LOG_DIR)"
	@echo "  CONCURRENCY               = $(CONCURRENCY)"
	@echo "  OUTPUT_FORMAT             = $(OUTPUT_FORMAT)"
	@echo "  INCLUDE_DATASTORE         = $(if $(INCLUDE_DATASTORE),yes,no)"
	@echo "  FROM_BLOCK                = $(FROM_BLOCK)"
	@echo "  TO_BLOCK                  = $(TO_BLOCK)"
	@echo "  MARKET                    = $(MARKET)"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	poetry install
	@echo "Done"

# ==============================================================================
# Full Collection (candles + oracle events)
# ==============================================================================

define COLLECT_CMD
	@echo "Starting $(1) candle collection..."
	@echo "  Output:     $(DATA_DIR)"
	@echo "  Concurrency: $(CONCURRENCY)"
	@echo ""
	@mkdir -p $(DATA_DIR) $(LOG_DIR)
	poetry run python -m gmx_historical_data.cli collect --$(2) --output-dir $(DATA_DIR) --concurrency $(CONCURRENCY)
endef

# Incremental candle collection (GMX API + Chainlink + oracle events)
# Uses smart gap detection; only fetches missing data
collect-update:
	$(call COLLECT_CMD,incremental,update)

# Full historical candle collection from genesis
collect-full:
	$(call COLLECT_CMD,full historical,full)

# Full incremental update: candles + funding (run this for daily updates)
update-gmx-data: collect-update funding-unified-resume
	@echo ""
	@echo "Update complete: candles + funding"

# Export candles + funding to FreqTrade feather format
export-freqtrade:
	@echo "Exporting to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	@echo ""
	@mkdir -p $(FEATHER_DIR)
	poetry run python -m gmx_historical_data.cli export-freqtrade --data-dir $(DATA_DIR) --output-dir $(FEATHER_DIR)

# ==============================================================================
# Unified Funding Extraction
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
