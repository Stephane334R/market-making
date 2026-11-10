"""Exact conversion of exchange decimal strings to integer ticks / lots.

Bybit sends prices and sizes as decimal strings ("74810.40", "0.001"). We store them
as int64 multiples of the instrument's step (tick size for prices, qty step for sizes)
so nothing is ever rounded through a float. A value that is not an exact multiple of
the step raises ``ValueError`` instead of being silently rounded: that is how a tick
size change in the history would surface.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

# BTCUSDT linear perpetual, from GET https://api.bybit.com/v5/market/instruments-info
# ?category=linear&symbol=BTCUSDT (priceFilter.tickSize / lotSizeFilter.qtyStep),
# checked on 2026-11-10. ASSUMPTION: these steps held over the whole date range we
# normalize. A historical change would make conversion fail loudly, not silently.
BTCUSDT_TICK_SIZE = "0.10"
BTCUSDT_LOT_SIZE = "0.001"


def to_units(value: str, step: str) -> int:
    """Return ``value / step`` as an int, raising if it is not an exact integer."""
    try:
        q = Decimal(value) / Decimal(step)
    except InvalidOperation as e:
        raise ValueError(f"not a decimal: {value!r}") from e
    if q != q.to_integral_value():
        raise ValueError(f"{value!r} is not a multiple of step {step!r}")
    return int(q)


def from_units(units: int, step: str) -> Decimal:
    """Inverse of :func:`to_units`, exact."""
    return Decimal(units) * Decimal(step)


class UnitCache(dict):
    """Memoizing converter: ``cache["74810.40"] -> 748104``.

    Order book files repeat the same price/size strings millions of times; a dict hit
    is far cheaper than a Decimal division. Subclassing dict with ``__missing__`` keeps
    the hot path (``cache[s]``) a plain C-level dict lookup.
    """

    def __init__(self, step: str):
        super().__init__()
        self.step = step

    def __missing__(self, key: str) -> int:
        v = to_units(key, self.step)
        self[key] = v
        return v
