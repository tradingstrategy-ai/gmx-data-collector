# ==============================================================================
# GMX Historical Data - Makefile
# ==============================================================================
# Override defaults on the command line: make full-data SYMBOL=ETH CONCURRENCY=8
# ==============================================================================

# Auto-load .env variables if .env exists.
# Shell environment variables take precedence — .env only fills in unset vars.
ifneq (,$(wildcard .env))
$(foreach line,$(shell grep -v '^\s*\#' .env | grep -v '^\s*$$' | sed 's/^export //' | sed 's/"//g'),\
  $(if $(filter undefined,$(origin $(firstword $(subst =, ,$(line))))),\
    $(eval export $(line))))
endif

# Network configuration
NETWORK ?= arbitrum

# Directory paths
DATA_DIR ?= ./user_data
UNIFIED_OUTPUT_DIR ?= $(DATA_DIR)/funding
OI_OUTPUT_DIR ?= $(DATA_DIR)/data/gmx/open_interest
POOL_LIQUIDITY_OUTPUT_DIR ?= $(DATA_DIR)/data/gmx/pool_liquidity
FEATHER_DIR ?= $(DATA_DIR)
LOG_DIR ?= ./logs
CHECKPOINT_DIR ?= ./checkpoints

# Collection options
CONCURRENCY ?= 5

# Extraction options
OUTPUT_FORMAT ?= parquet
FROM_BLOCK ?=
TO_BLOCK ?=
MARKET ?=
SYMBOL ?=
ARGS ?=

# Set INCLUDE_DATASTORE=1 to include archive RPC reads (slow, requires JSON_RPC_ARBITRUM)
INCLUDE_DATASTORE ?=

# ==============================================================================
# Targets
# ==============================================================================

.PHONY: help install show-config monitor \
        refresh-data full-data \
        collect-update collect-full export-freqtrade \
        funding-unified funding-unified-resume funding-unified-merge \
        funding-feather funding-full \
        oi oi-resume \
        pool-liquidity pool-liquidity-resume \
        extract-all extract-all-resume

# Default target
help:
	@echo "GMX Historical Data"
	@echo ""
	@echo "Recommended targets:"
	@echo "  refresh-data    Incremental update (daily use): candles + funding + OI + liquidity + export"
	@echo "  full-data       Full historical download from genesis + export"
	@echo ""
	@echo "Individual targets:"
	@echo "  collect-update       Candles: incremental (GMX API + Chainlink + oracle)"
	@echo "  collect-full         Candles: full historical from genesis"
	@echo "  funding-unified      Funding: all phases (HyperSync, fast)"
	@echo "  funding-unified-resume  Funding: incremental (resume from checkpoints)"
	@echo "  funding-full         Funding: all phases + DataStore (slow, archive RPC)"
	@echo "  funding-feather      Funding: export to FreqTrade feather format"
	@echo "  funding-unified-merge  Funding: merge only (no re-extraction)"
	@echo "  oi                   Open Interest: full historical from genesis"
	@echo "  oi-resume            Open Interest: incremental (resume from checkpoint)"
	@echo "  pool-liquidity       Pool Liquidity: full historical from genesis"
	@echo "  pool-liquidity-resume  Pool Liquidity: incremental (resume from checkpoint)"
	@echo "  export-freqtrade     Export candles + funding to FreqTrade format"
	@echo ""
	@echo "Utility:"
	@echo "  install         Install dependencies with Poetry"
	@echo "  show-config     Show all configuration variables"
	@echo "  monitor         Live resource usage of running collector processes"
	@echo ""
	@echo "Examples:"
	@echo "  make refresh-data                           # Daily incremental update"
	@echo "  make full-data                              # First-time full download"
	@echo "  make collect-full SYMBOL=ETH                # Single symbol"
	@echo "  make full-data CONCURRENCY=8                # Faster with more workers"
	@echo "  make oi FROM_BLOCK=120000000                # OI from specific block"
	@echo ""
	@echo "Tip: .env is auto-loaded (HYPERSYNC_API_TOKEN, JSON_RPC_ARBITRUM, etc.)"

# ==============================================================================
# One-Command Targets
# ==============================================================================

# Incremental data refresh — tops up existing data with latest candles,
# funding rates, OI, liquidity, then exports to FreqTrade format.
refresh-data: collect-update funding-unified-resume extract-all-resume export-freqtrade
	@echo ""
	@echo "Incremental refresh complete: candles + funding + OI + liquidity + FreqTrade export"
	@echo "Data ready in $(DATA_DIR)"

# Full historical download — collects everything from genesis. Slow but complete.
full-data: collect-full funding-unified extract-all export-freqtrade
	@echo ""
	@echo "Full data download complete: candles + funding + OI + liquidity + FreqTrade export"
	@echo "Data ready in $(DATA_DIR)"

# ==============================================================================
# Candle Collection
# ==============================================================================

define COLLECT_CMD
	@echo "Starting $(1) candle collection..."
	@echo "  Output:      $(DATA_DIR)"
	@echo "  Concurrency: $(CONCURRENCY)"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	$(if $(ARGS),@echo "  Extra args:  $(ARGS)",)
	@echo ""
	@mkdir -p $(DATA_DIR) $(LOG_DIR)
	poetry run python -m gmx_historical_data.cli collect --$(2) \
		--output-dir $(DATA_DIR) \
		--concurrency $(CONCURRENCY) \
		--nice \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(ARGS)
endef

collect-update:
	$(call COLLECT_CMD,incremental,update)

collect-full:
	$(call COLLECT_CMD,full historical,full)

export-freqtrade:
	@echo "Exporting to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	@echo ""
	@mkdir -p $(FEATHER_DIR)
	poetry run python -m gmx_historical_data.cli export-freqtrade --data-dir $(DATA_DIR) --output-dir $(FEATHER_DIR)

# ==============================================================================
# Open Interest Extraction
# ==============================================================================

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

extract-all: oi pool-liquidity
	@echo ""
	@echo "Full extraction complete: OI + pool liquidity"

extract-all-resume: oi-resume pool-liquidity-resume
	@echo ""
	@echo "Incremental extraction complete: OI + pool liquidity"

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

funding-unified:
	@echo "Starting unified funding rate extraction (HyperSync only)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR)
	$(UNIFIED_CMD)

funding-full:
	@echo "Starting full funding rate extraction (HyperSync + DataStore)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR) $(CHECKPOINT_DIR)
	$(UNIFIED_CMD) --include-datastore

funding-unified-resume:
	@echo "Starting incremental unified funding rate extraction..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p $(UNIFIED_OUTPUT_DIR) $(LOG_DIR)
	$(UNIFIED_CMD) --resume

funding-unified-merge:
	@echo "Merging existing funding rate data..."
	@mkdir -p $(UNIFIED_OUTPUT_DIR)
	poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir $(UNIFIED_OUTPUT_DIR) \
		--merge-only \
		$(if $(MARKET),--market $(MARKET),)

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
# Utility
# ==============================================================================

install:
	@echo "Installing dependencies with Poetry..."
	poetry install
	@echo "Done"

monitor:
	@PIDS=$$(pgrep -f "gmx_historical_data" 2>/dev/null); \
	if [ -z "$$PIDS" ]; then \
		echo "No gmx_historical_data processes running."; \
	else \
		echo "Live monitoring (Ctrl+C to stop)"; \
		echo ""; \
		while true; do \
			PIDS=$$(pgrep -f "gmx_historical_data" 2>/dev/null); \
			if [ -z "$$PIDS" ]; then \
				echo "Process exited."; \
				break; \
			fi; \
			printf "\033[2J\033[H"; \
			echo "gmx_historical_data — $$(date '+%H:%M:%S')"; \
			echo ""; \
			echo "PID       RSS (MB)   CPU%  Command"; \
			echo "--------  ---------  ----  -------"; \
			ps -o pid=,rss=,%cpu=,command= -p $$PIDS 2>/dev/null | while read pid rss cpu cmd; do \
				rss_mb=$$(echo "scale=1; $$rss / 1024" | bc); \
				echo "$$pid  $$rss_mb MB    $$cpu%  $$(echo $$cmd | cut -c1-60)"; \
			done; \
			sleep 2; \
		done; \
	fi

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
	@echo "  SYMBOL                    = $(SYMBOL)"
	@echo "  ARGS                      = $(ARGS)"
