# Market Simulator — Approach & Code Structure

The default market data source (no `MASSIVE_API_KEY` required). Generates
realistic, correlated, live-updating stock prices in-process. This document
describes the design implemented in `backend/app/market/simulator.py` and
`backend/app/market/seed_prices.py`, and how it plugs into the unified
interface described in `planning/MARKET_DATA_API.md`.

## 1. Why a simulator

Per `planning/PLAN.md` §6, the simulator is the primary, supported data
source for this project — not a fallback bolted on for when a paid API key
is missing. It needs no external dependency, no network calls, no rate
limits, and produces the ~500ms-cadence price action the frontend's flash
animations and sparklines are built around, which a 15s-polled free-tier
Massive feed cannot deliver on its own.

## 2. The model: Geometric Brownian Motion

Each ticker's price evolves under GBM:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` — current price
- `mu` — annualized drift (expected return)
- `sigma` — annualized volatility
- `dt` — time step, expressed as a fraction of a trading year
- `Z` — a (correlated) standard normal random draw

### Choosing `dt`

Ticks happen every 500ms, but `mu`/`sigma` are annualized, so `dt` has to
convert "half a second" into "fraction of a trading year":

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8
```

This tiny `dt` is what keeps individual ticks to realistic sub-cent moves
that accumulate into believable multi-minute price action, rather than
each tick looking like a full day's move.

### Correlated moves via Cholesky decomposition

Real markets don't move ticker-by-ticker independently — tech stocks tend
to move together, as do financials. To reproduce that, draw `n` independent
standard normals, then multiply by the Cholesky factor of a correlation
matrix built from sector groupings:

```python
z_independent = np.random.standard_normal(n)
z_correlated = cholesky_factor @ z_independent   # correlated draws
```

Correlation structure (`app/market/seed_prices.py`):

| Pair | Correlation |
|---|---|
| Two tech tickers (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Two finance tickers (JPM, V) | 0.5 |
| Either ticker is TSLA | 0.3 (TSLA "does its own thing") |
| Cross-sector / unknown ticker | 0.3 |

The correlation matrix — and its Cholesky factor — is rebuilt whenever a
ticker is added or removed (O(n²), fine for n < ~50 watchlist tickers).

### Random shock events

Independently of the GBM step, each ticker has a small per-tick chance
(`event_probability`, default 0.1%) of a sudden 2–5% move in either
direction — this is what produces the occasional dramatic single-ticker
spike/drop for visual interest, layered on top of the smooth GBM walk. At
10 tickers and 2 ticks/sec, expect roughly one event every ~50 seconds.

```python
if random.random() < self._event_prob:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

## 3. Seed data (`app/market/seed_prices.py`)

Starting prices and per-ticker GBM parameters for the default watchlist —
volatility (`sigma`) and drift (`mu`) are hand-tuned per ticker to feel
representative (e.g. TSLA/NVDA high-vol, JPM/V low-vol):

```python
SEED_PRICES = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # high volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # high volatility, strong drift
    "JPM":  {"sigma": 0.18, "mu": 0.04},   # low volatility (bank)
    # ...
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}  # used for dynamically added tickers
```

A ticker added later that isn't in `SEED_PRICES`/`TICKER_PARAMS` (e.g. a
user adds an arbitrary symbol via the watchlist or chat) gets a random seed
price in `[$50, $300]` and `DEFAULT_PARAMS` — it still participates fully in
the simulation, just without hand-tuned realism.

## 4. Code structure

### 4.1 `GBMSimulator` — pure simulation state, no I/O

The math and per-ticker state live in a plain class with no asyncio, no
cache, no network — this keeps the numerically interesting part unit
testable in isolation.

```python
class GBMSimulator:
    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None: ...

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Hot path — called every
        500ms, so keep it allocation-light."""

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker; rebuilds the Cholesky decomposition."""

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker; rebuilds the Cholesky decomposition."""

    def get_price(self, ticker: str) -> float | None: ...
    def get_tickers(self) -> list[str]: ...
```

`step()` returns `{ticker: new_price}` for every tracked ticker on every
call — the caller decides what to do with that (write to a cache, print to
a terminal demo, feed a test assertion).

### 4.2 `SimulatorDataSource` — the `MarketDataSource` adapter

Wraps `GBMSimulator` in the async lifecycle the unified interface expects
(see `planning/MARKET_DATA_API.md` §3.2), and owns writing results into the
shared `PriceCache`.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache,
                 update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None: ...

    async def start(self, tickers: list[str]) -> None:
        """Build the GBMSimulator, seed the cache immediately (so SSE has
        data on the very first frame), then spawn the update loop task."""

    async def stop(self) -> None:
        """Cancel the update loop task; safe to call more than once."""

    async def add_ticker(self, ticker: str) -> None:
        """Delegate to GBMSimulator.add_ticker, then seed the cache with
        its starting price immediately rather than waiting for the next
        tick."""

    async def remove_ticker(self, ticker: str) -> None:
        """Delegate to GBMSimulator.remove_ticker, then remove from cache."""

    def get_tickers(self) -> list[str]: ...

    async def _run_loop(self) -> None:
        """while True: step the simulator, write every result to the
        cache, sleep(update_interval). Wrapped in try/except so one bad
        step (should never happen, but) doesn't kill the background task —
        it logs and continues on the next tick."""
```

Two "seed immediately" touches matter for UX: on `start()` and on
`add_ticker()`, the cache gets a value before the first scheduled tick
fires, so a newly opened SSE connection or a freshly added watchlist ticker
never shows as blank/missing for up to 500ms.

## 5. Example usage

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)   # SimulatorDataSource, since
                                             # MASSIVE_API_KEY is unset
await source.start(["AAPL", "GOOGL", "MSFT", "TSLA"])

# ... 500ms later ...
update = cache.get("TSLA")
print(update.price, update.direction, update.change_percent)

await source.add_ticker("NVDA")   # immediately visible in the cache
await source.remove_ticker("MSFT")

await source.stop()
```

A standalone terminal visualization of this exact simulator is available at
`backend/market_data_demo.py` (`uv run market_data_demo.py` from
`backend/`) — a live Rich dashboard with sparklines, direction arrows, and
an event log, useful for eyeballing whether tuning changes to `sigma`/`mu`
or the correlation groups still feel realistic.

## 6. Testing notes

Because `GBMSimulator` has no I/O, its statistical properties (drift over
many steps trends toward `mu`, correlated tickers actually correlate, an
added/removed ticker rebuilds the Cholesky matrix without crashing) are
directly testable with straightforward `pytest` assertions over many
`step()` calls — see `backend/tests/market/test_simulator.py` for the
existing suite (17 tests, 98% coverage per
`planning/MARKET_DATA_SUMMARY.md`).
