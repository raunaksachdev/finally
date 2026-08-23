# Unified Market Data API — Design

This is the interface design for retrieving stock prices in FinAlly: one
abstraction with two implementations — a live client against the Massive
API (see `planning/MASSIVE_API.md`) and a built-in simulator (see
`planning/MARKET_SUMULATOR.md`) — selected at startup by whether
`MASSIVE_API_KEY` is set. This document describes the interface as already
implemented in `backend/app/market/`; treat it as the contract reference
for that package.

## 1. Goals

- Downstream code (SSE stream, portfolio valuation, trade execution) never
  knows or cares whether prices come from Massive or the simulator.
- Switching sources is a pure environment-variable toggle — no code change,
  no restart-time branching beyond the factory.
- A single shared, thread-safe cache is the only thing downstream code reads
  from; data sources only ever write to it.

## 2. Component diagram

```
                 MASSIVE_API_KEY set?
                         │
        ┌────────────────┴────────────────┐
        │ yes                              │ no
        ▼                                  ▼
MassiveDataSource                  SimulatorDataSource
(REST poller, Massive API)         (GBM simulator, in-process)
        │                                  │
        └────────────────┬─────────────────┘
                          ▼
                     PriceCache
                (thread-safe, in-memory)
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   SSE stream        Portfolio        Trade execution
  (/api/stream/       valuation        (fill price)
     prices)
```

Both implementations conform to the same `MarketDataSource` abstract base
class, so the factory function is the only place that knows both classes
exist.

## 3. Core types

### 3.1 `PriceUpdate` (`app/market/models.py`)

An immutable snapshot of one ticker's price at a point in time. Frozen so it
can be shared across threads/tasks without defensive copying.

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float: ...          # price - previous_price
    @property
    def change_percent(self) -> float: ...  # % change vs previous_price
    @property
    def direction(self) -> str: ...          # "up" | "down" | "flat"

    def to_dict(self) -> dict: ...           # JSON-serializable for SSE
```

### 3.2 `MarketDataSource` (`app/market/interface.py`)

The abstract contract both implementations satisfy.

```python
class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.
        Starts a background task that periodically writes to the PriceCache.
        Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.
        Takes effect on the next update cycle."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set and from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

Design choices baked into this contract:
- **Async lifecycle, sync accessor** — `start`/`stop`/`add_ticker`/`remove_ticker`
  are async because both implementations do I/O or task management on those
  paths (spawning a poller, spawning the sim loop); `get_tickers()` is sync
  because it's just a local list read.
- **No `get_price()` on the interface** — price reads always go through
  `PriceCache`, never through the source. This keeps "who writes" (sources)
  and "who reads" (everyone else) strictly separated and means adding a
  third data source later requires zero changes to any reader.
- **Idempotent `stop()`, no-op `add_ticker`/`remove_ticker` on duplicates** —
  callers (route handlers, chat action execution) don't need to
  pre-check state before calling.

### 3.3 `PriceCache` (`app/market/cache.py`)

The single point of truth. Producers (one at a time — either
`SimulatorDataSource` or `MassiveDataSource`, never both) write; every
reader in the app reads from here, never from the source directly.

```python
class PriceCache:
    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate: ...
    def get(self, ticker: str) -> PriceUpdate | None: ...
    def get_price(self, ticker: str) -> float | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...
    def remove(self, ticker: str) -> None: ...

    @property
    def version(self) -> int: ...  # monotonically increasing, bumped on every update
```

- Guarded by a `threading.Lock` — safe even though writers and the FastAPI
  event loop can interleave (`MassiveDataSource` moves its synchronous HTTP
  call to a thread via `asyncio.to_thread`).
- `version` exists purely so the SSE endpoint can cheaply detect "did
  anything change since I last sent a frame" without diffing the whole
  price dict every tick.
- `update()` computes `previous_price` internally from whatever was cached
  before — callers only ever supply the new price; this is what makes
  `direction`/`change` correct without every writer duplicating that logic.

### 3.4 Factory (`app/market/factory.py`)

The only place that imports both concrete implementations and the only
place that reads `MASSIVE_API_KEY`.

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> MassiveDataSource.
    Otherwise -> SimulatorDataSource.
    Returns an unstarted source; caller must await source.start(tickers)."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

Trimming and checking for non-empty (not just presence) matters: an `.env`
file with `MASSIVE_API_KEY=` (present but blank) must fall back to the
simulator, not attempt a poll with an empty key.

## 4. Implementations

### 4.1 `SimulatorDataSource` — default

Runs an in-process asyncio task that steps a GBM model every ~500ms and
writes results straight into `PriceCache`. No network calls, no external
dependency, no API key. Full design in `planning/MARKET_SUMULATOR.md`.

### 4.2 `MassiveDataSource` — optional, when `MASSIVE_API_KEY` is set

Polls Massive's multi-ticker snapshot endpoint (`get_snapshot_all`) on a
timer — 15s on the free tier, 2–5s on paid tiers — fetching all watched
tickers in a single REST call, then writes `last_trade.price` /
`last_trade.timestamp` into the cache. Full research and code examples in
`planning/MASSIVE_API.md`.

Per `planning/PLAN.md` §6 and `CLAUDE.md`, this project deliberately ships
with `MASSIVE_API_KEY` unset — the simulator is the supported path — and
this implementation is kept working but not actively extended.

## 5. Wiring into the app

```python
from app.market import PriceCache, create_market_data_source, create_stream_router

# App startup
price_cache = PriceCache()
market_source = create_market_data_source(price_cache)
await market_source.start(initial_watchlist_tickers)

app.include_router(create_stream_router(price_cache))

# Watchlist mutation (REST route or LLM tool call) — same call either way
await market_source.add_ticker("TSLA")
await market_source.remove_ticker("GOOGL")

# App shutdown
await market_source.stop()
```

`create_stream_router(price_cache)` (`app/market/stream.py`) builds the SSE
endpoint (`GET /api/stream/prices`) as a closure over the cache, so the
endpoint has no dependency on which `MarketDataSource` is active. The
streaming loop polls `price_cache.version` every 500ms and only emits a
frame when it has advanced — this is what lets `add_ticker`/`remove_ticker`
apply to an already-open SSE connection without a reconnect: the next tick
after the cache changes just includes (or drops) that ticker.

Portfolio valuation and trade execution read current prices the same way —
`price_cache.get_price(ticker)` — so they too are agnostic to the data
source.

## 6. Extending this later

Adding a third source (a different vendor, a WebSocket-based feed, etc.)
requires only: implement `MarketDataSource`, and extend the factory's
branch on the relevant env var. No other module changes, because every
consumer already goes through `PriceCache`.
