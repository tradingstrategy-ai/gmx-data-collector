"""Auto-tune concurrency and batch parameters based on system resources.

Detects available CPU cores and memory, then returns conservative defaults
that avoid starving the host. Also lowers process priority (nice) so the
collector yields CPU to other services.
"""

import logging
import os
import platform

logger = logging.getLogger(__name__)


def _get_available_memory_mb() -> int:
    """Return available physical memory in MB.

    :return: Available memory in MB, or 0 if detection fails.
    """
    try:
        import psutil

        return int(psutil.virtual_memory().available / (1024 * 1024))
    except ImportError:
        pass

    # Fallback: read from sysctl (macOS) or /proc/meminfo (Linux)
    if platform.system() == "Darwin":
        try:
            import subprocess

            out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
            total_bytes = int(out.strip())
            # Estimate 60% available (no reliable "free" metric without psutil on macOS)
            return int(total_bytes * 0.6 / (1024 * 1024))
        except Exception:
            pass
    else:
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) // 1024
        except Exception:
            pass

    return 0


def _get_cpu_count() -> int:
    """Return number of available CPU cores.

    :return: CPU core count, minimum 1.
    """
    return max(os.cpu_count() or 1, 1)


def apply_nice() -> None:
    """Lower process priority so collector doesn't starve other services.

    Sets nice to 10 (low priority) on Unix systems. Silently ignored on
    Windows or if permission is denied.
    """
    try:
        os.nice(10)
        logger.info("Process priority lowered (nice +10)")
    except (OSError, AttributeError):
        pass


def get_resource_limits() -> dict:
    """Auto-detect system resources and return tuned parameters.

    Returns a dict with recommended values for:
    - ``concurrency``: parallel chunk/symbol workers
    - ``timeframe_concurrency``: parallel timeframe fetches per symbol
    - ``chainlink_concurrency``: Chainlink RPC batch workers
    - ``block_cache_workers``: block timestamp cache fetch workers
    - ``flush_every``: oracle event flush threshold

    :return: Dict of parameter name to recommended value.
    """
    cpus = _get_cpu_count()
    mem_mb = _get_available_memory_mb()

    # CPU-based limits: reserve at least 2 cores for the OS and other services
    usable_cpus = max(cpus - 2, 1)

    # Memory-based limits
    if mem_mb == 0:
        # Detection failed — use conservative defaults
        mem_tier = "unknown"
        mem_concurrency_cap = 2
        flush_every = 200_000
    elif mem_mb < 2048:
        mem_tier = "low (<2GB)"
        mem_concurrency_cap = 1
        flush_every = 100_000
    elif mem_mb < 4096:
        mem_tier = "medium (2-4GB)"
        mem_concurrency_cap = 2
        flush_every = 250_000
    elif mem_mb < 8192:
        mem_tier = "good (4-8GB)"
        mem_concurrency_cap = 4
        flush_every = 500_000
    else:
        mem_tier = "high (>8GB)"
        mem_concurrency_cap = 6
        flush_every = 500_000

    # Final values: minimum of CPU-based and memory-based limits
    concurrency = min(usable_cpus, mem_concurrency_cap)
    timeframe_concurrency = min(usable_cpus, 6, mem_concurrency_cap)
    chainlink_concurrency = min(usable_cpus, 3, mem_concurrency_cap)
    block_cache_workers = min(usable_cpus * 4, 32, mem_concurrency_cap * 8)

    limits = {
        "concurrency": max(concurrency, 1),
        "timeframe_concurrency": max(timeframe_concurrency, 1),
        "chainlink_concurrency": max(chainlink_concurrency, 1),
        "block_cache_workers": max(block_cache_workers, 1),
        "flush_every": flush_every,
    }

    logger.info(
        f"Resource limits: {cpus} CPUs, {mem_mb}MB available ({mem_tier}), "
        f"concurrency={limits['concurrency']}, "
        f"timeframe_concurrency={limits['timeframe_concurrency']}, "
        f"flush_every={limits['flush_every']:,}"
    )

    return limits
