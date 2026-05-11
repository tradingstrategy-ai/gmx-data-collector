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
# DAT_DIR: shorthand override that sets both source (DATA_DIR) and export
# (FEATHER_DIR) to the same path. Explicit DATA_DIR / FEATHER_DIR on the CLI
# still take precedence.
DAT_DIR ?=
ifneq ($(strip $(DAT_DIR)),)
DATA_DIR ?= $(DAT_DIR)
FEATHER_DIR ?= $(DAT_DIR)
endif
DATA_DIR ?= /Volumes/WD Blue 1tb/VMs/data/gmx
UNIFIED_OUTPUT_DIR ?= $(DATA_DIR)/funding
OI_OUTPUT_DIR ?= $(DATA_DIR)/open_interest
POOL_LIQUIDITY_OUTPUT_DIR ?= $(DATA_DIR)/pool_liquidity
FEATHER_DIR ?= /Volumes/WD Blue 1tb/VMs/data
LOG_DIR ?= ./logs
CHECKPOINT_DIR ?= ./checkpoints

# Collection options
CONCURRENCY ?= 5

# Run all commands at reduced priority to avoid saturating the server
NICE ?= nice -n 10

# Extraction options
OUTPUT_FORMAT ?= parquet
FROM_BLOCK ?=
TO_BLOCK ?=
MARKET ?=
SYMBOL ?=
ARGS ?=

# Set INCLUDE_DATASTORE=1 to include archive RPC reads (slow, requires JSON_RPC_ARBITRUM)
INCLUDE_DATASTORE ?=

# Pass --keep to export-freqtrade to preserve source parquet after export (default: delete)
KEEP ?=
# Pass --overwrite to replace existing feather files entirely instead of merging
OVERWRITE ?=

# CEX gap-fill knobs (additive, optional)
GAP_THRESHOLD    ?= 0.20
MERGE_GAP_BARS   ?= 2
CEX_DATADIR      ?=
CEX_EXCHANGES    ?= binance,bybit
CEX_ROUTING_FILE ?= configs/cex_routing.json
SKIP_DOWNLOAD    ?=

# ==============================================================================
# Targets
# ==============================================================================

.PHONY: help install show-config monitor \
        refresh-data full-data full-data-nn \
        collect-update collect-update-nn collect-full collect-full-nn export-freqtrade \
        funding-unified funding-unified-nn funding-unified-resume funding-unified-merge \
        funding-feather funding-full \
        oi oi-resume \
        pool-liquidity pool-liquidity-resume \
        extract-all extract-all-resume \
        fill-gaps-cex refresh-data-cex full-data-cex full-data-nn-cex

# Default target
help:
	@echo "GMX Historical Data"
	@echo ""
	@echo "Recommended targets:"
	@echo "  refresh-data    Incremental update (daily use): candles + funding + OI + liquidity + export"
	@echo "  full-data       Full historical download from genesis + export"
	@echo "  full-data-nn    Full historical download, no nice, concurrency 10 + export"
	@echo "  refresh-data-cex   Incremental + CEX gap-fill (Binance/Bybit) + export"
	@echo "  full-data-cex      Full historical + CEX gap-fill + export"
	@echo "  full-data-nn-cex   Full historical no-nice + CEX gap-fill + export"
	@echo ""
	@echo "Individual targets:"
	@echo "  collect-update       Candles: incremental (GMX API + Chainlink + oracle)"
	@echo "  collect-update-nn    Candles: incremental, no nice, concurrency 10 (nn = no-nice)"
	@echo "  collect-full         Candles: full historical from genesis, nice+10 (enable quickstart with QUICKSTART=--quickstart)"
	@echo "  collect-full-nn      Candles: full historical, no nice, concurrency 10 (nn = no-nice)"
	@echo "  funding-unified      Funding: all phases (HyperSync, fast)"
	@echo "  funding-unified-nn   Funding: all phases, no nice (nn = no-nice, max throughput)"
	@echo "  funding-unified-resume  Funding: incremental (resume from checkpoints)"
	@echo "  funding-full         Funding: all phases + DataStore (slow, archive RPC)"
	@echo "  funding-feather      Funding: export to FreqTrade feather format"
	@echo "  funding-unified-merge  Funding: merge only (no re-extraction)"
	@echo "  oi                   Open Interest: full historical from genesis"
	@echo "  oi-resume            Open Interest: incremental (resume from checkpoint)"
	@echo "  pool-liquidity       Pool Liquidity: full historical from genesis"
	@echo "  pool-liquidity-resume  Pool Liquidity: incremental (resume from checkpoint)"
	@echo "  export-freqtrade     Export candles + funding to FreqTrade format"
	@echo "  fill-gaps-cex        Run CEX gap-fill stage on existing parquet"
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

# Full historical download, no nice — maximum throughput (dedicated machine / overnight).
full-data-nn: collect-full-nn funding-unified extract-all export-freqtrade
	@echo ""
	@echo "Full data download complete (no-nice): candles + funding + OI + liquidity + FreqTrade export"
	@echo "Data ready in $(DATA_DIR)"

# ==============================================================================
# Candle Collection
# ==============================================================================

define COLLECT_CMD
	@echo "Starting $(1) candle collection..."
	@echo "  Output:      $(DATA_DIR)"
	@echo "  Concurrency: $(CONCURRENCY)"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	$(if $(3),@echo "  Quickstart:  $(3) (seeds ./user_data/ from data/daily-collection branch)",)
	$(if $(ARGS),@echo "  Extra args:  $(ARGS)",)
	@echo ""
	@mkdir -p "$(DATA_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli collect --$(2) \
		--output-dir "$(DATA_DIR)" \
		--concurrency $(CONCURRENCY) \
		--nice \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(3) \
		$(ARGS)
endef

collect-update:
	$(call COLLECT_CMD,incremental,update,)

# No-nice variant: no OS-level nice, no --nice auto-tune, concurrency 10.
collect-update-nn:
	@echo "Starting incremental candle collection (no-nice, concurrency 10)..."
	@echo "  Output:      $(DATA_DIR)"
	@echo "  Concurrency: 10"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	$(if $(ARGS),@echo "  Extra args:  $(ARGS)",)
	@echo ""
	@mkdir -p "$(DATA_DIR)" "$(LOG_DIR)"
	poetry run python -m gmx_historical_data.cli collect --update \
		--output-dir "$(DATA_DIR)" \
		--concurrency 10 \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(ARGS)

# --quickstart is OFF BY DEFAULT. Enable with QUICKSTART=--quickstart when
# invoking: `make collect-full QUICKSTART=--quickstart`.
QUICKSTART ?=
collect-full:
	$(call COLLECT_CMD,full historical,full,$(QUICKSTART))

# No-nice variant: no OS-level nice, no --nice auto-tune, concurrency 10.
# Use this when you want maximum throughput (e.g. dedicated machine / overnight run).
collect-full-nn:
	@echo "Starting full historical candle collection (no-nice, concurrency 10)..."
	@echo "  Output:      $(DATA_DIR)"
	@echo "  Concurrency: 10"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	$(if $(QUICKSTART),@echo "  Quickstart:  $(QUICKSTART)",)
	$(if $(ARGS),@echo "  Extra args:  $(ARGS)",)
	@echo ""
	@mkdir -p "$(DATA_DIR)" "$(LOG_DIR)"
	poetry run python -m gmx_historical_data.cli collect --full \
		--output-dir "$(DATA_DIR)" \
		--concurrency 10 \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(QUICKSTART) \
		$(ARGS)

export-freqtrade:
	@echo "Exporting to FreqTrade format..."
	@echo "  Data:       $(DATA_DIR)"
	@echo "  Output:     $(FEATHER_DIR)"
	@echo ""
	@mkdir -p "$(FEATHER_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli export-freqtrade \
		--data-dir "$(DATA_DIR)" \
		--output-dir "$(FEATHER_DIR)" \
		$(KEEP) \
		$(OVERWRITE)

# ==============================================================================
# Open Interest Extraction
# ==============================================================================

oi:
	@echo "Starting GMX V2 Open Interest extraction (full history)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(OI_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(OI_OUTPUT_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python scripts/extract_open_interest.py \
		--network $(NETWORK) \
		--output-dir "$(OI_OUTPUT_DIR)" \
		--output parquet \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),--from-block 120000000) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

oi-resume:
	@echo "Starting incremental OI extraction (resume from checkpoint)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(OI_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(OI_OUTPUT_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python scripts/extract_open_interest.py \
		--network $(NETWORK) \
		--output-dir "$(OI_OUTPUT_DIR)" \
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
	@mkdir -p "$(POOL_LIQUIDITY_OUTPUT_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python scripts/extract_pool_liquidity.py \
		--network $(NETWORK) \
		--output-dir "$(POOL_LIQUIDITY_OUTPUT_DIR)" \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),--from-block 120000000) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

pool-liquidity-resume:
	@echo "Starting incremental Pool Liquidity extraction (resume from checkpoint)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(POOL_LIQUIDITY_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(POOL_LIQUIDITY_OUTPUT_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python scripts/extract_pool_liquidity.py \
		--network $(NETWORK) \
		--output-dir "$(POOL_LIQUIDITY_OUTPUT_DIR)" \
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
$(NICE) poetry run python scripts/extract_unified_funding.py \
	--network $(NETWORK) \
	--output-dir "$(UNIFIED_OUTPUT_DIR)" \
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
	@mkdir -p "$(UNIFIED_OUTPUT_DIR)" "$(LOG_DIR)"
	$(UNIFIED_CMD)

funding-full:
	@echo "Starting full funding rate extraction (HyperSync + DataStore)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(UNIFIED_OUTPUT_DIR)" "$(LOG_DIR)" "$(CHECKPOINT_DIR)"
	$(UNIFIED_CMD) --include-datastore

funding-unified-resume:
	@echo "Starting incremental unified funding rate extraction..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(UNIFIED_OUTPUT_DIR)" "$(LOG_DIR)"
	$(UNIFIED_CMD) --resume

# No-nice variant: no OS-level nice, maximum throughput (dedicated machine / overnight).
funding-unified-nn:
	@echo "Starting unified funding rate extraction (no-nice, HyperSync only)..."
	@echo "  Network:    $(NETWORK)"
	@echo "  Output:     $(UNIFIED_OUTPUT_DIR)"
	@echo ""
	@mkdir -p "$(UNIFIED_OUTPUT_DIR)" "$(LOG_DIR)"
	poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir "$(UNIFIED_OUTPUT_DIR)" \
		--output $(OUTPUT_FORMAT) \
		$(if $(INCLUDE_DATASTORE),--include-datastore,) \
		$(if $(FROM_BLOCK),--from-block $(FROM_BLOCK),) \
		$(if $(TO_BLOCK),--to-block $(TO_BLOCK),) \
		$(if $(MARKET),--market $(MARKET),)

funding-unified-merge:
	@echo "Merging existing funding rate data..."
	@mkdir -p "$(UNIFIED_OUTPUT_DIR)"
	$(NICE) poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir "$(UNIFIED_OUTPUT_DIR)" \
		--merge-only \
		$(if $(MARKET),--market $(MARKET),)

funding-feather:
	@echo "Exporting to FreqTrade feather format..."
	@echo "  Source:     $(UNIFIED_OUTPUT_DIR)"
	@echo "  Feather:    $(FEATHER_DIR)"
	@echo ""
	@mkdir -p "$(FEATHER_DIR)"
	$(NICE) poetry run python scripts/extract_unified_funding.py \
		--network $(NETWORK) \
		--output-dir "$(UNIFIED_OUTPUT_DIR)" \
		--output feather \
		--feather-dir "$(FEATHER_DIR)" \
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
	@echo "  GAP_THRESHOLD             = $(GAP_THRESHOLD)"
	@echo "  MERGE_GAP_BARS            = $(MERGE_GAP_BARS)"
	@echo "  CEX_EXCHANGES             = $(CEX_EXCHANGES)"
	@echo "  CEX_ROUTING_FILE          = $(CEX_ROUTING_FILE)"
	@echo "  CEX_DATADIR               = $(CEX_DATADIR)"

# ==============================================================================
# CEX Gap-Fill (additive — runs between collect and export-freqtrade)
# ==============================================================================

fill-gaps-cex:
	@echo "Filling GMX price gaps with CEX data..."
	@echo "  Data dir:    $(DATA_DIR)"
	@echo "  Threshold:   $(GAP_THRESHOLD)"
	@echo "  Exchanges:   $(CEX_EXCHANGES)"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	@mkdir -p "$(DATA_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli fill-gaps-cex \
		--data-dir "$(DATA_DIR)" \
		--gap-threshold $(GAP_THRESHOLD) \
		--merge-gap-bars $(MERGE_GAP_BARS) \
		--exchanges $(CEX_EXCHANGES) \
		--routing-file "$(CEX_ROUTING_FILE)" \
		$(if $(CEX_DATADIR),--cex-datadir "$(CEX_DATADIR)",) \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(if $(SKIP_DOWNLOAD),--skip-download,) \
		$(ARGS)

refresh-data-cex: collect-update funding-unified-resume extract-all-resume fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Incremental refresh (with CEX gap-fill) complete"

full-data-cex: collect-full funding-unified extract-all fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Full data download (with CEX gap-fill) complete"

full-data-nn-cex: collect-full-nn funding-unified extract-all fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Full data download no-nice (with CEX gap-fill) complete"
