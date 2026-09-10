"""Inclusive/exclusive block-range handling for HyperSync queries.

HyperSync's ``Query.to_block`` is **exclusive** -- the returned range is
``[from_block, to_block)`` -- while every caller in this repo thinks in
inclusive ranges. That is not an accident of style: an inclusive end is what a
CLI ``--from-block``/``--to-block`` (or ``--start-block``/``--end-block``) pair
means, what a checkpoint's ``last_block`` means, and what a chunk's ``ce`` means
when it is computed as ``start + size - 1``.

Passing an inclusive end straight through therefore dropped the last block of
every range, silently and one block at a time. Verified live against Arbitrum
(``arbitrum.hypersync.xyz``, 2026-09-10): querying ``[b, b]`` for a block
holding two matching logs returns **0 logs**, while ``[b, b+1]`` returns 2.

The damage depended entirely on what the caller did next:

- ``trade_tick_collector`` scanned ``[checkpoint + 1, tip]`` and then advanced
  the checkpoint to ``tip``, so the tip block's fills were lost for good --
  once per daily run.
- ``oracle_price_collector`` splits a range into ``concurrency`` chunks and
  passes each chunk's inclusive ``ce``, so every chunk lost its own last block
  and no chunk covered it: ``concurrency`` blocks per full-history scan.
- ``scripts/extract_*.py --resume`` advanced the checkpoint to the requested
  ``to_block`` on a run that found nothing, skipping a block that was never
  scanned.

Rather than repeat ``end_block + 1`` at each site -- which is how this spread in
the first place -- the conversion lives here once, and every query builder calls
it. ``None`` means "to the end of data" to HyperSync and is passed through
untouched.
"""


def exclusive_end(inclusive_end: int | None) -> int | None:
    """Convert an inclusive last block to HyperSync's exclusive ``to_block``.

    :param inclusive_end: The last block the caller wants covered, inclusive.
        ``None`` means "as far as the data goes", which HyperSync expresses by
        leaving ``to_block`` unset.
    :returns: The value to pass as ``Query.to_block``: ``inclusive_end + 1``,
        or ``None`` when ``inclusive_end`` is ``None``.
    """
    if inclusive_end is None:
        return None
    return inclusive_end + 1
