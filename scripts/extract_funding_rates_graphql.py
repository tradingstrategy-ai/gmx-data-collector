#!/usr/bin/env python3
"""
GMX V2 Historical Funding Rate Extractor (Subsquid GraphQL Edition)
====================================================================

Extracts historical funding and borrowing rate data from GMX V2 using
the Subsquid GraphQL indexer — no ABI decoding required.

Data sources:
  - fundingRateSnapshots:   fundingFactorPerSecond (2025-08-19 — present)
  - borrowingRateSnapshots: borrowingFactorPerSecond (2023-07-05 — present)

QUICK START
-----------
    # Fetch ALL tokens from the oldest available date to now (default)
    python scripts/extract_funding_rates_graphql.py

    # Specific market and date range
    python scripts/extract_funding_rates_graphql.py --market ETH/USD --from-date 2024-01-01

USAGE
-----
    python scripts/extract_funding_rates_graphql.py [OPTIONS]

OPTIONS
-------
    --from-date    Start date YYYY-MM-DD (default: 2023-07-05 — oldest available)
    --to-date      End date YYYY-MM-DD (default: today)
    --market       Filter by market symbol (e.g., "ETH/USD", "BTC/USD")
    --output       Output format: "parquet", "json", or "csv" (default: parquet)
    --output-dir   Output directory (default: current directory)
    --interval     Aggregation interval: "raw", "1h", "8h" (default: raw)
    --data-type    Which rate to extract: "funding", "borrowing", "both" (default: both)
    --list-markets List available markets and exit

EXAMPLES
--------
    # Default: all tokens, full history, parquet output
    python scripts/extract_funding_rates_graphql.py

    # ETH/USD only, specific date range
    python scripts/extract_funding_rates_graphql.py --market ETH/USD --from-date 2024-01-01 --to-date 2025-01-01

    # All markets, 8-hour aggregated, CSV output
    python scripts/extract_funding_rates_graphql.py --interval 8h --output csv

    # List available markets
    python scripts/extract_funding_rates_graphql.py --list-markets

DATA NOTES
----------
    Funding rate snapshots (fundingFactorPerSecondLong/Short) are available
    from ~August 2025 onwards. Before that date, only borrowing rate snapshots
    are available.

    All rates are stored with 10^30 precision on-chain.
    Conversions:
      hourly rate = factorPerSecond * 3600
      8h rate     = factorPerSecond * 3600 * 8
      annual rate = factorPerSecond * 3600 * 24 * 365
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
# CONSTANTS
# =============================================================================

SUBSQUID_URL = "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql"
PRECISION = 10**30
PAGE_SIZE = 1000

# Earliest available data in Subsquid (borrowingRateSnapshots)
OLDEST_AVAILABLE_DATE = "2023-07-05"

# Well-known index token addresses → symbol mapping (Arbitrum, checksummed)
INDEX_TOKEN_SYMBOLS = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": "ETH",
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": "BTC",
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": "LINK",
    "0x912CE59144191C1204E64559FE8253a0e49E6548": "ARB",
    "0x2bcC6D6CdBbDC0a4071e48bb3B969b06B3330c07": "SOL",
    "0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0": "UNI",
    "0xC4da4c24fd591125c3F47b340b6f4f76111883d8": "DOGE",
    "0xB46A094Bc4B0adBD801E14b9DB95e05E28962764": "LTC",
    "0xc14e065b0067dE91534e032868f5Ac6ecf2c6868": "XRP",
    "0x565609fAF65B92F7be02468acF86f8979423e514": "AVAX",
    "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196": "AAVE",
    "0xaC800FD6159c2a2CB8fC31EF74621eB430287a5A": "OP",
    "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a": "GMX",
    "0x25d887Ce7a35172C62FeBFD67a1856F20FaEbB00": "PEPE",
    "0xA1b91fe9FD52141Ff8cac388Ce3F10BFDc1dE79d": "WIF",
    "0xa9004A5421372E1d83fB1f85b0fc986c912f91f3": "BNB",
    "0x1FF7F3EFBb9481Cbd7db4F932cBCD4467144237C": "NEAR",
    "0x7D7F1765aCbaF847b9A1f7137FE8Ed4931FbfEbA": "ATOM",
    "0x47904963fc8b2340414262125aF798B9655E58Cd": "WBTC",
}


# =============================================================================
# GRAPHQL CLIENT
# =============================================================================

def graphql_query(query: str, variables: Optional[dict] = None) -> dict:
    """Execute a GraphQL query against the Subsquid endpoint."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    data = json.dumps(payload).encode("utf-8")
    req = Request(
        SUBSQUID_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "gmx-funding-rate-extractor/1.0",
        },
    )

    for attempt in range(3):
        try:
            with urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            break
        except URLError as e:
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                print(f"  Retry {attempt + 1}/3 after {wait}s: {e}", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"Error querying Subsquid after 3 attempts: {e}", file=sys.stderr)
                sys.exit(1)

    if "errors" in result:
        for err in result["errors"]:
            print(f"GraphQL error: {err['message']}", file=sys.stderr)
        sys.exit(1)

    return result["data"]


# =============================================================================
# MARKET RESOLUTION
# =============================================================================

def fetch_markets() -> dict[str, dict]:
    """
    Fetch all markets from the subgraph and build address → info mapping.
    Uses original address case from the subgraph (checksummed).
    """
    data = graphql_query("""
    {
        markets(limit: 200) {
            id
            indexToken
            longToken
            shortToken
        }
    }
    """)

    market_map = {}
    for m in data["markets"]:
        addr = m["id"]  # Keep original case
        idx = m["indexToken"]
        symbol_base = INDEX_TOKEN_SYMBOLS.get(idx, idx[:10])
        symbol = f"{symbol_base}/USD"
        market_map[addr] = {
            "symbol": symbol,
            "indexToken": idx,
            "longToken": m["longToken"],
            "shortToken": m["shortToken"],
        }
    return market_map


def list_markets(market_map: dict) -> None:
    """Print all available markets."""
    print(f"\n{'Symbol':<14} {'Market Address':<44}")
    print("-" * 58)
    seen = set()
    for addr, info in sorted(market_map.items(), key=lambda x: x[1]["symbol"]):
        key = info["symbol"]
        if key in seen:
            # Some symbols have multiple markets (different collateral)
            key = f"{info['symbol']} ({addr[:10]}...)"
        seen.add(key)
        print(f"{key:<14} {addr}")
    print(f"\nTotal: {len(market_map)} markets")


# =============================================================================
# BORROWING RATE EXTRACTION (from launch — 2023-07)
# =============================================================================

def fetch_borrowing_snapshots(
    from_ts: int,
    to_ts: int,
    target_addrs: Optional[set] = None,
    market_map: Optional[dict] = None,
) -> list[dict]:
    """Fetch borrowing rate snapshots with cursor-based pagination."""
    all_records = []
    last_id = ""
    page = 0

    while True:
        page += 1
        where_parts = [
            f'snapshotTimestamp_gte: {from_ts}',
            f'snapshotTimestamp_lte: {to_ts}',
        ]
        if last_id:
            where_parts.append(f'id_gt: "{last_id}"')
        if target_addrs:
            addr_list = ", ".join(f'"{a}"' for a in target_addrs)
            where_parts.append(f'address_in: [{addr_list}]')

        where_clause = ", ".join(where_parts)

        query = f"""
        {{
            borrowingRateSnapshots(
                limit: {PAGE_SIZE},
                orderBy: id_ASC,
                where: {{ {where_clause} }}
            ) {{
                id
                address
                borrowingFactorPerSecondLong
                borrowingFactorPerSecondShort
                snapshotTimestamp
            }}
        }}
        """

        data = graphql_query(query)
        snapshots = data["borrowingRateSnapshots"]

        if not snapshots:
            break

        for s in snapshots:
            addr = s["address"]
            info = market_map.get(addr, {"symbol": f"UNKNOWN({addr[:10]})"})

            long_per_sec = int(s["borrowingFactorPerSecondLong"]) / PRECISION
            short_per_sec = int(s["borrowingFactorPerSecondShort"]) / PRECISION
            ts = s["snapshotTimestamp"]

            record = {
                "symbol": info["symbol"],
                "marketAddress": addr,
                "timestamp": ts,
                "datetime": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "rateType": "borrowing",
                "factorPerSecondLong": long_per_sec,
                "factorPerSecondShort": short_per_sec,
                "rateHourlyLong": long_per_sec * 3600,
                "rateHourlyShort": short_per_sec * 3600,
                "rate8hLong": long_per_sec * 3600 * 8,
                "rate8hShort": short_per_sec * 3600 * 8,
                "rateAnnualizedLong": long_per_sec * 3600 * 24 * 365,
                "rateAnnualizedShort": short_per_sec * 3600 * 24 * 365,
            }
            all_records.append(record)

        last_id = snapshots[-1]["id"]
        print(f"  [borrowing] Page {page}: {len(snapshots)} snapshots (total: {len(all_records)})", end="\r")

        if len(snapshots) < PAGE_SIZE:
            break

    print(f"\n  [borrowing] Total: {len(all_records)} snapshots")
    return all_records


# =============================================================================
# FUNDING RATE EXTRACTION (from ~Aug 2025)
# =============================================================================

def fetch_funding_snapshots(
    from_ts: int,
    to_ts: int,
    target_addrs: Optional[set] = None,
    market_map: Optional[dict] = None,
) -> list[dict]:
    """Fetch funding rate snapshots with cursor-based pagination."""
    all_records = []
    last_id = ""
    page = 0

    while True:
        page += 1
        where_parts = [
            f'snapshotTimestamp_gte: {from_ts}',
            f'snapshotTimestamp_lte: {to_ts}',
        ]
        if last_id:
            where_parts.append(f'id_gt: "{last_id}"')
        if target_addrs:
            addr_list = ", ".join(f'"{a}"' for a in target_addrs)
            where_parts.append(f'marketAddress_in: [{addr_list}]')

        where_clause = ", ".join(where_parts)

        query = f"""
        {{
            fundingRateSnapshots(
                limit: {PAGE_SIZE},
                orderBy: id_ASC,
                where: {{ {where_clause} }}
            ) {{
                id
                marketAddress
                fundingFactorPerSecondLong
                fundingFactorPerSecondShort
                snapshotTimestamp
            }}
        }}
        """

        data = graphql_query(query)
        snapshots = data["fundingRateSnapshots"]

        if not snapshots:
            break

        for s in snapshots:
            addr = s["marketAddress"]
            info = market_map.get(addr, {"symbol": f"UNKNOWN({addr[:10]})"})

            long_per_sec = int(s["fundingFactorPerSecondLong"]) / PRECISION
            short_per_sec = int(s["fundingFactorPerSecondShort"]) / PRECISION
            ts = s["snapshotTimestamp"]

            record = {
                "symbol": info["symbol"],
                "marketAddress": addr,
                "timestamp": ts,
                "datetime": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "rateType": "funding",
                "factorPerSecondLong": long_per_sec,
                "factorPerSecondShort": short_per_sec,
                "rateHourlyLong": long_per_sec * 3600,
                "rateHourlyShort": short_per_sec * 3600,
                "rate8hLong": long_per_sec * 3600 * 8,
                "rate8hShort": short_per_sec * 3600 * 8,
                "rateAnnualizedLong": long_per_sec * 3600 * 24 * 365,
                "rateAnnualizedShort": short_per_sec * 3600 * 24 * 365,
            }
            all_records.append(record)

        last_id = snapshots[-1]["id"]
        print(f"  [funding] Page {page}: {len(snapshots)} snapshots (total: {len(all_records)})", end="\r")

        if len(snapshots) < PAGE_SIZE:
            break

    print(f"\n  [funding] Total: {len(all_records)} snapshots")
    return all_records


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_snapshots(records: list[dict], interval: str) -> list[dict]:
    """Average snapshots into periodic intervals."""
    if interval == "raw":
        return records

    interval_seconds = 3600 if interval == "1h" else 3600 * 8

    # Group by (symbol, rateType, bucket)
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        bucket_ts = (r["timestamp"] // interval_seconds) * interval_seconds
        key = (r["symbol"], r["rateType"], bucket_ts)
        buckets[key].append(r)

    aggregated = []
    for (symbol, rate_type, bucket_ts), recs in sorted(buckets.items()):
        n = len(recs)
        avg_long = sum(r["factorPerSecondLong"] for r in recs) / n
        avg_short = sum(r["factorPerSecondShort"] for r in recs) / n

        aggregated.append({
            "symbol": symbol,
            "marketAddress": recs[0]["marketAddress"],
            "timestamp": bucket_ts,
            "datetime": datetime.fromtimestamp(bucket_ts, tz=timezone.utc).isoformat(),
            "rateType": rate_type,
            "factorPerSecondLong": avg_long,
            "factorPerSecondShort": avg_short,
            "rateHourlyLong": avg_long * 3600,
            "rateHourlyShort": avg_short * 3600,
            "rate8hLong": avg_long * 3600 * 8,
            "rate8hShort": avg_short * 3600 * 8,
            "rateAnnualizedLong": avg_long * 3600 * 24 * 365,
            "rateAnnualizedShort": avg_short * 3600 * 24 * 365,
            "snapshotCount": n,
        })

    return aggregated


# =============================================================================
# OUTPUT
# =============================================================================

def save_parquet(records: list[dict], filepath: str) -> None:
    """Save records to Parquet file using PyArrow."""
    if not records:
        print("No records to save.")
        return

    # Build columnar arrays from records
    columns = list(records[0].keys())
    arrays = {}
    for col in columns:
        values = [r[col] for r in records]
        if col == "timestamp":
            arrays[col] = pa.array(values, type=pa.int64())
        elif col == "snapshotCount":
            arrays[col] = pa.array(values, type=pa.uint32())
        elif isinstance(values[0], float):
            arrays[col] = pa.array(values, type=pa.float64())
        else:
            arrays[col] = pa.array(values, type=pa.string())

    table = pa.table(arrays)
    pq.write_table(table, filepath, compression="snappy")
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Saved {len(records)} records to {filepath} ({file_size_mb:.1f} MB)")


def save_json(records: list[dict], filepath: str) -> None:
    """Save records to JSON file."""
    with open(filepath, "w") as f:
        json.dump(records, f, indent=2, default=str)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Saved {len(records)} records to {filepath} ({file_size_mb:.1f} MB)")


def save_csv(records: list[dict], filepath: str) -> None:
    """Save records to CSV file."""
    if not records:
        print("No records to save.")
        return
    fieldnames = list(records[0].keys())
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Saved {len(records)} records to {filepath} ({file_size_mb:.1f} MB)")


def print_summary(records: list[dict]) -> None:
    """Print a summary table of the extracted data."""
    print("\n" + "=" * 90)
    print("GMX V2 RATE SUMMARY")
    print("=" * 90)

    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in records:
        by_key[(r["symbol"], r["rateType"])].append(r)

    print(f"\n{'Symbol':<14} {'Type':<10} {'Snapshots':>10} {'Avg 8h Long':>14} {'Avg 8h Short':>14} {'Avg Ann Long':>14}")
    print("-" * 80)

    for (symbol, rate_type) in sorted(by_key.keys()):
        recs = by_key[(symbol, rate_type)]
        avg_8h_long = sum(r["rate8hLong"] for r in recs) / len(recs)
        avg_8h_short = sum(r["rate8hShort"] for r in recs) / len(recs)
        avg_ann_long = sum(r["rateAnnualizedLong"] for r in recs) / len(recs)

        print(f"{symbol:<14} {rate_type:<10} {len(recs):>10} {avg_8h_long:>13.8f}% {avg_8h_short:>13.8f}% {avg_ann_long:>13.4f}%")

    timestamps = [r["timestamp"] for r in records]
    if timestamps:
        first = datetime.fromtimestamp(min(timestamps), tz=timezone.utc)
        last = datetime.fromtimestamp(max(timestamps), tz=timezone.utc)
        print(f"\nTime range: {first.isoformat()} → {last.isoformat()}")
    print(f"Total records: {len(records)}")


# =============================================================================
# MARKET FILTER HELPER
# =============================================================================

def resolve_market_filter(market_filter: str, market_map: dict) -> set[str]:
    """Resolve a market symbol filter to a set of market addresses."""
    target_addrs = set()
    market_filter_upper = market_filter.upper()

    # Exact match first
    for addr, info in market_map.items():
        if info["symbol"].upper() == market_filter_upper:
            target_addrs.add(addr)

    if not target_addrs:
        # Partial match
        for addr, info in market_map.items():
            if market_filter_upper in info["symbol"].upper():
                target_addrs.add(addr)
        if target_addrs:
            matches = [market_map[a]["symbol"] for a in target_addrs]
            print(f"  No exact match for '{market_filter}', using partial matches: {matches}")
        else:
            print(f"  Warning: No market found for '{market_filter}'", file=sys.stderr)

    return target_addrs


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 historical funding & borrowing rates via Subsquid GraphQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    default_to = datetime.now(tz=timezone.utc)

    parser.add_argument("--from-date", type=str, default=OLDEST_AVAILABLE_DATE,
                        help=f"Start date YYYY-MM-DD (default: {OLDEST_AVAILABLE_DATE} — oldest available)")
    parser.add_argument("--to-date", type=str, default=default_to.strftime("%Y-%m-%d"),
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--market", type=str, default=None,
                        help="Filter by market symbol (e.g., 'ETH/USD', 'BTC/USD'). Default: all markets")
    parser.add_argument("--output", choices=["parquet", "json", "csv"], default="parquet",
                        help="Output format (default: parquet)")
    parser.add_argument("--output-dir", type=str, default=".",
                        help="Output directory (default: current directory)")
    parser.add_argument("--interval", choices=["raw", "1h", "8h"], default="raw",
                        help="Aggregation interval (default: raw)")
    parser.add_argument("--data-type", choices=["funding", "borrowing", "both"], default="both",
                        help="Which rate to extract (default: both)")
    parser.add_argument("--list-markets", action="store_true",
                        help="List available markets and exit")

    args = parser.parse_args()

    # Fetch market info (always needed)
    print("Fetching market info...")
    market_map = fetch_markets()
    print(f"  Found {len(market_map)} markets")

    if args.list_markets:
        list_markets(market_map)
        sys.exit(0)

    # Parse dates
    from_dt = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    to_dt = datetime.strptime(args.to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    from_ts = int(from_dt.timestamp())
    to_ts = int(to_dt.timestamp())

    print("\n" + "=" * 90)
    print("GMX V2 HISTORICAL RATE EXTRACTOR (Subsquid GraphQL)")
    print("=" * 90)
    print(f"Date range:  {from_dt.date()} → {to_dt.date()}")
    print(f"Data type:   {args.data_type}")
    print(f"Interval:    {args.interval}")
    print(f"Market:      {args.market or 'ALL'}")
    print(f"Output:      {args.output}")
    print("=" * 90)

    # Resolve market filter
    target_addrs = None
    if args.market:
        target_addrs = resolve_market_filter(args.market, market_map)
        if not target_addrs:
            print("No matching markets found. Use --list-markets to see available options.")
            sys.exit(1)
        print(f"  Matched addresses: {target_addrs}")

    all_records = []
    t0 = time.time()

    # Fetch borrowing rates (available from 2023-07-05)
    if args.data_type in ("borrowing", "both"):
        print("\nFetching borrowing rate snapshots (available from 2023-07)...")
        borrowing = fetch_borrowing_snapshots(from_ts, to_ts, target_addrs, market_map)
        all_records.extend(borrowing)

    # Fetch funding rates (available from ~2025-08-19)
    if args.data_type in ("funding", "both"):
        print("\nFetching funding rate snapshots (available from ~2025-08)...")
        funding = fetch_funding_snapshots(from_ts, to_ts, target_addrs, market_map)
        all_records.extend(funding)
        if not funding and from_ts < 1755608400:  # 2025-08-19
            print("  Note: Funding rate snapshots are only available from ~August 2025.")
            print("  For older data, borrowing rates are available via --data-type borrowing")

    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed:.1f}s")

    if not all_records:
        print("\nNo rate data found for the given parameters.")
        sys.exit(0)

    # Sort by timestamp
    all_records.sort(key=lambda r: (r["timestamp"], r["symbol"], r["rateType"]))

    # Aggregate
    if args.interval != "raw":
        print(f"\nAggregating to {args.interval} intervals...")
        all_records = aggregate_snapshots(all_records, args.interval)
        print(f"  {len(all_records)} aggregated records")

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Save
    market_tag = args.market.replace("/", "-") if args.market else "all"
    filename = f"gmx_rates_{args.data_type}_{market_tag}_{args.from_date}_{args.to_date}_{args.interval}"
    filepath = f"{args.output_dir}/{filename}.{args.output}"

    if args.output == "parquet":
        save_parquet(all_records, filepath)
    elif args.output == "json":
        save_json(all_records, filepath)
    else:
        save_csv(all_records, filepath)

    # Summary
    print_summary(all_records)


if __name__ == "__main__":
    main()
