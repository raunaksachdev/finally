# Market Data Backend — Detailed Design

Implementation-ready design for the FinAlly market data subsystem: the
unified `MarketDataSource` interface, the GBM simulator (default source),
the Massive (Polygon.io) REST client (optional source), the shared price
cache, the SSE streaming endpoint, and how all of it wires into the FastAPI
app.

**Status:** this subsystem is already built and tested — see
`planning/MARKET_DATA_SUMMARY.md` for the test/coverage summary. This
document is the implementation-ready reference: every code block below is
taken from (or matches) the real source under `backend/app/market/`, so it
doubles as an onboarding doc and as a spec a fresh implementation could be
rebuilt from. It supersedes `planning/archive/MARKET_DATA_DESIGN.md`, whose
code had drifted slightly from the implementation after later fixes
(notably: `massive_client.py` no longer lazy-imports `massive`, and
`GBMSimulator.get_tickers()` is now a public method).

Everything below lives under `backend/app/market/`.

---

## Table of Contents

1. [Goals & Component Diagram](#1-goals--component-diagram)
2. [Data Model — `models.py`](#2-data-model)
3. [Price Cache — `cache.py`](#3-price-cache)
4. [Abstract Interface — `interface.py`](#4-abstract-interface)
5. [Seed Prices & Ticker Parameters — `seed_prices.py`](#5-seed-prices--ticker-parameters)
6. [GBM Simulator — `simulator.py`](#6-gbm-simulator)
7. [Massive API Client — `massive_client.py`](#7-massive-api-client)
8. [Factory — `factory.py`](#8-factory)
9. [SSE Streaming Endpoint — `stream.py`](#9-sse-streaming-endpoint)
10. [FastAPI Lifecycle Integration](#10-fastapi-lifecycle-integration)
11. [Watchlist Coordination](#11-watchlist-coordination)
12. [Testing Strategy](#12-testing-strategy)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Configuration Summary](#14-configuration-summary)

---

## 1. Goals & Component Diagram

- Downstream code (SSE stream, portfolio valuation, trade execution) never
  knows or cares whether prices come from Massive or the simulator.
- Switching sources is a pure environment-variable toggle (`MASSIVE_API_KEY`)
  — no code change, no restart-time branching beyond the factory function.
- A single shared, thread-safe cache is the only thing downstream code reads
  from; data sources only ever write to it.

```
                 MASSIVE_API_KEY set (non-empty)?
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
class (strategy pattern), so `factory.py` is the only module that imports
both concrete classes and the only one that reads `MASSIVE_API_KEY`.

### File layout

```
backend/
  app/
    market/
      __init__.py             # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                               # create_market_data_source, create_stream_router
      models.py                # PriceUpdate dataclass
      cache.py                 # PriceCache (thread-safe in-memory store)
      interface.py             # MarketDataSource ABC
      seed_prices.py            # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS
      simulator.py              # GBMSimulator + SimulatorDataSource
      massive_client.py         # MassiveDataSource
      factory.py                 # create_market_data_source()
      stream.py                  # SSE endpoint (FastAPI router factory)
  tests/
    market/                      # test_models.py, test_cache.py, test_simulator.py,
                                  # test_simulator_source.py, test_factory.py, test_massive.py
  market_data_demo.py             # Rich terminal demo of the live simulator
```

---

## 2. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only data structure that leaves the market data layer.
Every downstream consumer — SSE streaming, portfolio valuation, trade
execution — works exclusively with this type.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`**: price updates are immutable value objects — safe to
  share across threads/async tasks without defensive copying.
- **`slots=True`**: memory optimization; many of these are created per second.
- **Computed properties** (`change`, `direction`, `change_percent`): derived
  from `price`/`previous_price` so they can never drift out of sync — there
  is no stale `direction` field to accidentally forget to update.
- **`to_dict()`**: single serialization point used by both the SSE endpoint
  and any REST API response that needs a price.

---

## 3. Price Cache

**File: `backend/app/market/cache.py`**

The central data hub. Data sources write to it; SSE streaming, portfolio
valuation, and trade execution read from it. It must be thread-safe because
`MassiveDataSource` runs its synchronous HTTP call via `asyncio.to_thread`
(a real OS thread), while everything else touches it from the asyncio event
loop.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a version counter?

The SSE streaming loop polls the cache every ~500ms. Without a version
counter it would serialize and send every price on every tick even when
nothing changed (e.g. Massive only updates every 15s). The counter lets the
loop skip a send when nothing is new:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

`update()` computes `previous_price` internally from whatever was cached
before — callers only ever supply the new price. That's what makes
`direction`/`change` correct without every writer duplicating that logic.

---

## 4. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Design choices baked into this contract

- **Async lifecycle, sync accessor** — `start`/`stop`/`add_ticker`/
  `remove_ticker` are `async` because both implementations do I/O or task
  management on those paths (spawning a poller / the sim loop);
  `get_tickers()` is sync because it's just a local list read.
- **No `get_price()` on the interface** — price reads always go through
  `PriceCache`, never through the source. This keeps "who writes" (sources)
  and "who reads" (everyone else) strictly separated; adding a third data
  source later requires zero changes to any reader.
- **Idempotent `stop()`, no-op `add_ticker`/`remove_ticker` on duplicates**
  — callers (route handlers, chat action execution) don't need to
  pre-check state before calling.
- **Push model, not pull** — the source decides its own timing internally
  (simulator ticks every 500ms, Massive polls every 15s) and just writes to
  the cache whenever it has something. The SSE layer never needs to know
  which source is active or how often it updates.

---

## 5. Seed Prices & Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports beyond stdlib types. Shared by the
simulator (initial prices + GBM parameters) and available as sane fallback
seed prices for anything that needs one.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

A ticker added later that isn't in `SEED_PRICES`/`TICKER_PARAMS` (a user
adds an arbitrary symbol via the watchlist or chat) gets a random seed price
in `[$50, $300]` and `DEFAULT_PARAMS` — it still participates fully in the
simulation, just without hand-tuned realism (see `_add_ticker_internal`
below).

---

## 6. GBM Simulator

**File: `backend/app/market/simulator.py`**

The default, primary data source — not a fallback bolted on for when a paid
API key is missing (see `planning/PLAN.md` §6). It needs no external
dependency beyond `numpy`, no network calls, no rate limits, and produces
the ~500ms-cadence price action the frontend's flash animations and
sparklines are built around, which a 15s-polled free-tier Massive feed
cannot deliver on its own.

Two classes live here:
- `GBMSimulator` — pure math engine, no I/O. Stateful: holds current prices
  and advances them one step at a time.
- `SimulatorDataSource` — the `MarketDataSource` adapter that wraps
  `GBMSimulator` in an async loop and writes results into the `PriceCache`.

### 6.1 The model — Geometric Brownian Motion

Each ticker's price evolves under GBM:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where `S(t)` is the current price, `mu` is annualized drift, `sigma` is
annualized volatility, `dt` is the time step as a fraction of a trading
year, and `Z` is a (correlated) standard normal random draw.

**Choosing `dt`**: ticks happen every 500ms, but `mu`/`sigma` are
annualized, so `dt` has to convert "half a second" into "fraction of a
trading year":

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8
```

This tiny `dt` is what keeps individual ticks to realistic sub-cent moves
that accumulate into believable multi-minute price action, rather than each
tick looking like a full day's move.

**Correlated moves via Cholesky decomposition**: real markets don't move
ticker-by-ticker independently — tech stocks tend to move together, as do
financials. Draw `n` independent standard normals, then multiply by the
Cholesky factor of a correlation matrix built from sector groupings:

```python
z_independent = np.random.standard_normal(n)
z_correlated = cholesky_factor @ z_independent   # correlated draws
```

| Pair | Correlation |
|---|---|
| Two tech tickers (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Two finance tickers (JPM, V) | 0.5 |
| Either ticker is TSLA | 0.3 (TSLA "does its own thing") |
| Cross-sector / unknown ticker | 0.3 |

The correlation matrix — and its Cholesky factor — is rebuilt whenever a
ticker is added or removed (O(n²), fine for n < ~50 watchlist tickers).

**Random shock events**: independently of the GBM step, each ticker has a
small per-tick chance (`event_probability`, default 0.1%) of a sudden 2–5%
move in either direction — the occasional dramatic single-ticker spike/drop
for visual interest, layered on top of the smooth GBM walk. At 10 tickers
and 2 ticks/sec, expect roughly one event every ~50 seconds.

### 6.2 `GBMSimulator` — the math engine

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

### 6.3 `SimulatorDataSource` — async wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding**: `start()` and `add_ticker()` both write to the
  cache *before* the loop's next tick, so the SSE endpoint (or a freshly
  added watchlist ticker) never shows blank/missing for up to 500ms.
- **Graceful cancellation**: `stop()` cancels the task and awaits it,
  catching `CancelledError` — clean shutdown during FastAPI lifespan
  teardown.
- **Exception resilience**: `_run_loop` catches exceptions per-step so one
  bad tick doesn't kill the whole background task.
- **Public accessor, not private-attribute reach-through**:
  `get_tickers()` delegates to `GBMSimulator.get_tickers()` rather than
  reading `self._sim._tickers` directly, keeping the class boundary clean.

### Example usage

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
an event log; useful for eyeballing whether tuning changes to `sigma`/`mu`
or the correlation groups still feel realistic.

---

## 7. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls Massive's (formerly Polygon.io) multi-ticker snapshot endpoint on a
timer. Per `planning/PLAN.md` §6 and `CLAUDE.md`, this project deliberately
ships with `MASSIVE_API_KEY` unset — the simulator is the supported,
primary path — but this client is kept working as a real alternative.

### 7.1 Endpoint used

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

Returns last trade, last quote, and day/previous-day OHLC for many tickers
in **one API call** — this is what makes polling viable even on the free
tier (5 req/min): a 10-ticker watchlist still costs one request per poll.

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()  # reads MASSIVE_API_KEY from the environment

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)
for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
```

Rate limits: **free tier → 5 req/min → poll every 15s** (default);
**paid tiers → poll every 2–5s**. See `planning/MASSIVE_API.md` for the full
endpoint catalog (single-ticker snapshot, previous close, custom bars,
last trade/quote) and error-code reference.

### 7.2 `MassiveDataSource`

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

`massive` (the `polygon-api-client` package's successor on PyPI, per the
October 2025 rebrand — see `planning/MASSIVE_API.md` §1) is imported at
**module level**, not lazily inside a method. It is declared as a core
backend dependency in `pyproject.toml`, so it is always installed
regardless of which data source ends up active at runtime; the
`MASSIVE_API_KEY` env var, not import structure, is what decides whether
`MassiveDataSource` is actually instantiated (see §8, Factory).

### 7.3 Error handling philosophy

The poller is intentionally resilient — it never lets a bad HTTP response
kill the background task:

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error. Poller keeps running (user might fix `.env` and restart). |
| **429 Rate Limited** | Logged as error. Next poll retries after `poll_interval` seconds. |
| **Network timeout** | Logged as error. Retries automatically on next cycle. |
| **Malformed snapshot** | Individual ticker skipped with a warning; other tickers still processed. |
| **All tickers fail** | Cache retains last-known prices. SSE keeps streaming stale data (better than no data). |

---

## 8. Factory

**File: `backend/app/market/factory.py`**

The only module that imports both concrete `MarketDataSource`
implementations and the only one that reads `MASSIVE_API_KEY`.

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

Trimming and checking for non-empty (not just presence) matters: an `.env`
file with `MASSIVE_API_KEY=` (present but blank) must fall back to the
simulator, not attempt a poll with an empty key.

### Usage at app startup

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g. ["AAPL", "GOOGL", ...]
```

### Extending this later

Adding a third source (a different vendor, a WebSocket-based feed, etc.)
requires only: implement `MarketDataSource`, and extend the factory's
branch on the relevant env var. No other module changes, because every
consumer already goes through `PriceCache`.

---

## 9. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

A FastAPI route that holds open a long-lived HTTP connection and pushes
price updates to the client as `text/event-stream`.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### SSE wire format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,...}}

```

The frontend parses this with the native `EventSource` API:

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices is { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
};
```

### Why poll-and-push instead of event-driven?

The SSE endpoint polls the cache on a fixed interval rather than being
notified by the data source. This is simpler and produces predictable,
evenly-spaced updates for the frontend, which accumulates them client-side
into sparkline charts — regular spacing matters for a clean line.

### Why watchlist changes need no reconnect

Because `add_ticker`/`remove_ticker` write straight into the same
`PriceCache` the stream reads from, the next tick after a watchlist change
just includes (or drops) that ticker — the SSE loop and the mutation are
fully decoupled through the cache, with no direct call path between them.

---

## 10. FastAPI Lifecycle Integration

The market data system starts and stops with the app via the `lifespan`
context manager.

**In `backend/app/main.py`:**

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""

    # --- STARTUP ---

    # 1. Create the shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Create and start the market data source
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial tickers from the database watchlist (lazy-init happens here too)
    initial_tickers = await load_watchlist_tickers()  # reads from SQLite
    await source.start(initial_tickers)

    # 4. Register the SSE streaming router
    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source():
    return app.state.market_source
```

### Accessing market data from other routes

Trade execution, portfolio valuation, and watchlist management access the
cache and data source via FastAPI dependency injection:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(trade: TradeRequest, price_cache: PriceCache = Depends(get_price_cache)):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(404, f"No price available for {trade.ticker}")
    # ... execute trade at current_price via the single execute_trade() function
    # (see planning/PLAN.md §9 — the chat action executor calls the same function) ...


@router.post("/watchlist")
async def add_to_watchlist(payload: WatchlistAdd, source=Depends(get_market_source)):
    # Insert into the watchlist table ...
    await source.add_ticker(payload.ticker)


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    # Delete from the watchlist table ...
    await source.remove_ticker(ticker)
```

---

## 11. Watchlist Coordination

### Flow: adding a ticker

```
User (or LLM) → POST /api/watchlist {ticker: "PYPL"}
  → Insert into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive: appends to ticker list, appears on next poll
  → Return success (ticker + current price if already available)
```

### Flow: removing a ticker

```
User (or LLM) → DELETE /api/watchlist/PYPL
  → Delete from watchlist table (SQLite)
  → await source.remove_ticker("PYPL")
      Simulator: removes from GBMSimulator, rebuilds Cholesky, removes from cache
      Massive: removes from ticker list, removes from cache
  → Return success
```

### Edge case: ticker has an open position

If the user removes a ticker from the watchlist but still holds shares, the
data source should keep tracking it so portfolio valuation stays accurate:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)

    # Only stop tracking if there's no open position
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

---

## 12. Testing Strategy

The existing suite lives in `backend/tests/market/` — 73 tests, all
passing, 91% overall coverage (per `planning/MARKET_DATA_SUMMARY.md` and
the independent verification in `planning/review.md`). Coverage is uneven:
`stream.py` sits around 33% because exercising the SSE generator properly
needs a running ASGI test client, not just unit tests — a real gap worth
closing, not a rounding error to wave off.

| Module | Tests | What it covers |
|--------|-------|-----------------|
| `test_models.py` | 11 | `PriceUpdate` properties (`change`, `direction`, `change_percent`), `to_dict()` |
| `test_cache.py` | 13 | `PriceCache` update/get/get_all/remove, version increments, first-update-is-flat |
| `test_simulator.py` | 17 | `GBMSimulator` math: positive prices, drift over many steps, add/remove ticker rebuilds Cholesky, unknown-ticker random seed |
| `test_simulator_source.py` | 10 | `SimulatorDataSource` async lifecycle: start seeds cache, prices update over time, clean stop, add/remove ticker |
| `test_factory.py` | 7 | env var branching (`MASSIVE_API_KEY` set/unset/blank) |
| `test_massive.py` | 13 | `MassiveDataSource` with the Massive client mocked — poll success, malformed snapshot skip, API error doesn't crash the loop |

### 12.1 Representative example — `GBMSimulator` math properties

```python
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:
    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_add_ticker_rebuilds_correlation(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # only 1 ticker, no correlation matrix needed
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None

    def test_unknown_ticker_gets_random_seed_price(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert 50.0 <= price <= 300.0
```

### 12.2 Representative example — `PriceCache`

```python
from app.market.cache import PriceCache


class TestPriceCache:
    def test_first_update_is_flat(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_version_increments(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
```

### 12.3 Representative example — `MassiveDataSource` (mocked)

```python
from unittest.mock import MagicMock, patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


@pytest.mark.asyncio
async def test_poll_updates_cache():
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
    source._tickers = ["AAPL", "GOOGL"]

    mock_snapshots = [
        _make_snapshot("AAPL", 190.50, 1707580800000),
        _make_snapshot("GOOGL", 175.25, 1707580800000),
    ]
    with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
        await source._poll_once()

    assert cache.get_price("AAPL") == 190.50
    assert cache.get_price("GOOGL") == 175.25
```

Because `_fetch_snapshots` is a plain instance method (not a lazily-imported
free function), tests patch it directly with `patch.object(source, ...)`
rather than patching a module-level `RESTClient` name — this is simpler now
that `massive` is a top-level import (see §7.2) and avoids the mock-target
fragility that an earlier revision of this design had.

### 12.4 Closing the `stream.py` gap

The recommended way to push `stream.py` coverage up is an ASGI-level
integration test using `httpx.ASGITransport`, reading a few chunks off the
response and asserting on the decoded SSE payload:

```python
import httpx
import pytest
from app.main import app


@pytest.mark.asyncio
async def test_sse_stream_emits_prices():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("GET", "/api/stream/prices") as response:
            assert response.status_code == 200
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    break
```

---

## 13. Error Handling & Edge Cases

### 13.1 Startup: empty watchlist

If the database has no watchlist entries, `start()` receives an empty list.
Both sources handle this gracefully — the simulator produces no prices, the
Massive poller skips its API call (`if not self._tickers: return`). The SSE
endpoint sends nothing until a ticker is added, at which point the source
starts tracking it immediately.

### 13.2 Price cache miss during trade

If a user tries to trade a ticker with no cached price yet (just added,
Massive hasn't polled):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {ticker}. Please wait a moment and try again.",
    )
```

The simulator avoids this in practice by seeding the cache synchronously
inside `add_ticker()`. The Massive client may have a brief gap between
`add_ticker()` returning and the next poll landing — the 400 with a clear
message is the correct response for that window.

### 13.3 Massive API key invalid

If the key is set but wrong, the first poll fails with 401. The poller logs
the error and keeps retrying every `poll_interval`. SSE keeps streaming
(connection healthy) but with no data. Fix: correct `.env` and restart —
there's no in-process key-reload mechanism, by design (this is a demo app,
not a service needing hot config reload).

### 13.4 Thread safety under load

`PriceCache` uses `threading.Lock` — a real mutex, correct across both the
event loop and the `asyncio.to_thread` worker thread the Massive client
runs on. Under normal load (≤50 tickers, 2 updates/sec) lock contention is
negligible; the critical section is a dict lookup and assignment. This is
intentionally not optimized further (e.g. no `ReadWriteLock`) — unneeded at
this project's scale.

### 13.5 Simulator numerical stability

- Prices are `round()`ed to 2 decimals in both `GBMSimulator.step()` and
  `PriceCache.update()`.
- The exponential formulation (`exp(drift + diffusion)`) guarantees prices
  stay positive — no explicit floor/clamp is needed.
- `dt` is tiny enough that even the shock-event path (`* (1 ± 0.02..0.05)`)
  can't push a price to zero or overflow in any realistic run length.

---

## 14. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (unset) | If set and non-empty, use Massive; otherwise use the simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` (seconds) | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` (seconds) | Time between Massive API polls (free-tier default) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` (seconds) | Time between SSE cache polls / pushes |
| SSE retry directive | `_generate_events()` | `1000` (ms) | Browser `EventSource` reconnection delay |

### `__init__.py` — public API surface

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

All downstream backend code — trade execution, watchlist routes, LLM chat
action execution, portfolio valuation — should import exclusively from
`app.market` (this `__init__.py`), never reach into submodules like
`app.market.simulator` directly. That's what keeps the rest of the backend
fully agnostic to which concrete data source is active.
