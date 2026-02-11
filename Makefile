.PHONY: help install funding-full funding-incremental funding-status funding-stop funding-logs funding-clean

# Default target
help:
	@echo "GMX Historical Data Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  install              - Install dependencies with Poetry"
	@echo "  funding-full         - Run full historical funding rate extraction (background)"
	@echo "  funding-incremental  - Run incremental funding rate extraction (background)"
	@echo "  funding-status       - Check status of background funding extraction"
	@echo "  funding-stop         - Stop background funding extraction"
	@echo "  funding-logs         - Tail funding extraction logs"
	@echo "  funding-clean        - Clean up temporary files and test data"
	@echo ""
	@echo "Usage:"
	@echo "  make install         # First time setup"
	@echo "  make funding-full    # Initial historical extraction"
	@echo "  make funding-incremental  # Daily/hourly updates"
	@echo "  make funding-status  # Check if extraction is running"
	@echo "  make funding-logs    # Watch extraction progress"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	poetry install

# Full historical extraction (background mode)
funding-full:
	@echo "Starting full historical funding rate extraction in background..."
	@mkdir -p logs data/funding checkpoints
	poetry run python scripts/extract_funding_rates.py \
		--output parquet \
		--output-dir ./data/funding \
		--resume \
		--checkpoint-dir ./checkpoints \
		--background \
		--log-file logs/funding.log \
		--pid-file logs/funding.pid
	@echo "Background extraction started. Check status with: make funding-status"
	@echo "Watch logs with: make funding-logs"

# Incremental extraction (for daily/hourly updates)
funding-incremental:
	@echo "Starting incremental funding rate extraction in background..."
	@mkdir -p logs data/funding checkpoints
	poetry run python scripts/extract_funding_rates.py \
		--output parquet \
		--output-dir ./data/funding \
		--resume \
		--checkpoint-dir ./checkpoints \
		--background \
		--log-file logs/funding.log \
		--pid-file logs/funding.pid
	@echo "Background extraction started. Check status with: make funding-status"
	@echo "Watch logs with: make funding-logs"

# Check status of background process
funding-status:
	@echo "Checking funding extraction status..."
	@if [ -f logs/funding.pid ]; then \
		PID=$$(cat logs/funding.pid); \
		if ps -p $$PID > /dev/null 2>&1; then \
			echo "✓ Funding extraction is running (PID: $$PID)"; \
			echo ""; \
			echo "Memory usage:"; \
			ps -p $$PID -o pid,vsz,rss,comm,args | tail -1; \
			echo ""; \
			echo "Latest log entries:"; \
			tail -5 logs/funding.log 2>/dev/null || echo "No logs yet"; \
		else \
			echo "✗ Funding extraction is NOT running (stale PID file)"; \
			echo "Last log entries:"; \
			tail -10 logs/funding.log 2>/dev/null || echo "No logs"; \
		fi \
	else \
		echo "✗ No PID file found. Extraction is not running."; \
	fi

# Stop background extraction
funding-stop:
	@echo "Stopping funding extraction..."
	@if [ -f logs/funding.pid ]; then \
		PID=$$(cat logs/funding.pid); \
		if ps -p $$PID > /dev/null 2>&1; then \
			kill $$PID && echo "✓ Sent stop signal to process $$PID"; \
			sleep 2; \
			if ps -p $$PID > /dev/null 2>&1; then \
				echo "⚠ Process still running, sending SIGKILL..."; \
				kill -9 $$PID; \
			fi; \
			rm logs/funding.pid; \
		else \
			echo "✗ Process not running (stale PID file)"; \
			rm logs/funding.pid; \
		fi \
	else \
		echo "✗ No PID file found"; \
	fi

# Tail logs in real-time
funding-logs:
	@echo "Tailing funding extraction logs (Ctrl+C to exit)..."
	@tail -f logs/funding.log 2>/dev/null || echo "No log file found at logs/funding.log"

# Clean up temporary files
funding-clean:
	@echo "Cleaning up temporary files..."
	@rm -rf test_* gmx_v2_funding_*.json
	@echo "✓ Cleaned up temporary files"
	@echo ""
	@echo "To clean data/funding and checkpoints, run manually:"
	@echo "  rm -rf data/funding checkpoints"
