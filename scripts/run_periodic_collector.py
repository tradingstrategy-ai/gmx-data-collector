#!/usr/bin/env python3
"""Standalone entry point for GMX periodic data collection daemon.

This script can be run directly or via poetry:
    python scripts/run_periodic_collector.py
    poetry run python scripts/run_periodic_collector.py

Or installed as a console script:
    poetry run gmx-periodic-collector
"""

if __name__ == "__main__":
    from gmx_historical_data.daemon.periodic_collector import main

    main()
