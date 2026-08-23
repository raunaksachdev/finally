# Massive API Research (formerly Polygon.io)

Research notes on the Massive.com API for retrieving real-time and end-of-day
prices for multiple tickers, as used by `MassiveDataSource` in
`backend/app/market/massive_client.py`. This is background/reference
documentation — per `CLAUDE.md`, FinAlly intentionally runs on the built-in
simulator by default and does not currently target further Massive-specific
work, but this doc keeps the integration accurate if `MASSIVE_API_KEY` is
ever set.

## 1. The rebrand

Polygon.io rebranded to **Massive** on **October 30, 2025**. Existing API
keys, accounts, and integrations continued to work without interruption.
The REST surface stayed at `api.polygon.io`-compatible routes, now also
served from `api.massive.com`; the Python package was renamed from
`polygon-api-client` to **`massive`** on PyPI.
[Source: Polygon.io is Now Massive](https://massive.com/blog/polygon-is-now-massive)

## 2. Installation & authentication

```bash
pip install -U massive
# or, in this project:
uv add massive
```

Requires Python 3.9+.

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment automatically
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")
```

Auth is a bearer token derived from the API key; the client attaches it to
every request automatically — callers never build the `Authorization`
header by hand.
[Source: massive-com/client-python README](https://github.com/massive-com/client-python/blob/master/README.md)

## 3. Rate limits & plans

| Plan | Requests | Data |
|---|---|---|
| Free | 5 requests/minute | 15-minute delayed |
| Starter / Developer / Advanced / Business (paid, from $199/mo) | No hard cap — Massive monitors usage rather than enforcing a fixed ceiling; stay under ~100 req/s to avoid throttling | Real-time or 15-min delayed depending on tier |

[Source: What is the request limit for Massive's RESTful APIs?](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis) ·
[Source: Massive Pricing](https://massive.com/pricing)

This confirms the polling cadence already specified in `planning/PLAN.md` §6
and implemented in `MassiveDataSource`: **free tier → poll every 15s**
(5 req/min ceiling with margin), **paid tiers → poll every 2–5s**.

## 4. Endpoints relevant to multi-ticker real-time + EOD prices

### 4.1 Full Market / Multi-Ticker Snapshot (primary endpoint used by this project)

Returns a snapshot (last trade, last quote, day OHLC, previous day OHLC) for
many tickers in **one API call** — this is what makes polling on the free
tier viable, since watching 10 tickers still costs only 1 request.

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

Query parameters:
- `tickers` (optional, comma-separated, case-sensitive) — restrict to specific symbols, e.g. `AAPL,TSLA,GOOG`. Omitting it returns the entire market (10,000+ tickers).
- `include_otc` (optional, bool) — include OTC securities; default `false`.

Snapshot data resets daily at 3:30 AM ET and starts repopulating as
exchanges report, from as early as 4:00 AM ET.

Response shape (per ticker, camelCase over the wire):

```json
{
  "ticker": "AAPL",
  "day": { "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07, "v": 111237700, "vw": 127.35 },
  "prevDay": { "o": 128.4, "h": 129.95, "l": 127.8, "c": 129.61, "v": 98765400, "vw": 128.9 },
  "lastTrade": { "p": 125.07, "s": 100, "x": 11, "t": 1675190399000 },
  "lastQuote": { "p": 125.06, "P": 125.08, "s": 500, "S": 1000, "t": 1675190399500 },
  "min": { "o": 125.0, "h": 125.1, "l": 124.95, "c": 125.07, "v": 12000 },
  "todaysChange": -4.54,
  "todaysChangePerc": -3.50,
  "updated": 1675190399500000000
}
```

Python client (models expose the same fields as snake_case attributes):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day change: {snap.day.change_percent}%")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Prev close: {snap.prev_daily_bar.close}")
```

Requires Starter plan or above (not available on the bare free tier for
delayed-only access in some configurations — verify against the account's
actual plan before relying on it in a paid deployment).
[Source: Full Market Snapshot docs](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)

### 4.2 Single Ticker Snapshot

Same shape as above, scoped to one ticker — used for a detail view when a
user clicks a specific ticker.

```python
snapshot = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)
print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
```
[Source: Single Ticker Snapshot docs](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot)

### 4.3 Previous Close (end-of-day)

```
GET /v2/aggs/ticker/{stocksTicker}/prev
```

Returns the prior trading day's OHLCV for one ticker — useful for seeding a
simulator with realistic starting prices, or for computing day-over-day
change independent of the snapshot endpoint.

```python
prev = client.get_previous_close_agg(ticker="AAPL")
for agg in prev:
    print(f"Previous close: ${agg.close}  O={agg.open} H={agg.high} L={agg.low}  V={agg.volume}")
```

Response (raw JSON):
```json
{
  "ticker": "AAPL",
  "results": [
    {"o": 150.0, "h": 155.0, "l": 149.0, "c": 154.5, "v": 1000000, "t": 1672531200000}
  ]
}
```
[Source: Previous Day Bar (OHLC) docs](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)

### 4.4 Aggregates / Custom Bars (historical, for charts)

Not needed for live polling, but the natural source for a "main chart"
history view beyond what's accumulated client-side from the SSE stream.

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

```python
aggs = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-31",
    limit=50000,
):
    aggs.append(a)
```
[Source: Custom Bars (OHLC) docs](https://massive.com/docs/rest/stocks/aggregates/custom-bars)

### 4.5 Last Trade / Last Quote (single values)

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid_price} x {quote.bid_size}  Ask: ${quote.ask_price} x {quote.ask_size}")
```
[Source: massive-com/client-python README](https://github.com/massive-com/client-python/blob/master/README.md)

## 5. Pagination

The client paginates automatically by default for list-style endpoints
(`list_aggs`, `list_trades`, `list_quotes`), transparently fetching
subsequent pages as the generator is iterated. Disable with
`RESTClient(api_key=..., pagination=False)` if manual paging is preferred.

## 6. Error handling

The client raises typed exceptions rather than returning error payloads:
- **401** — invalid or missing API key
- **403** — valid key, but the current plan doesn't include this endpoint/data tier
- **429** — rate limit exceeded (free tier: 5 req/min)
- **5xx** — server error; treat as transient

`MassiveDataSource._poll_once()` (see `backend/app/market/massive_client.py`)
already wraps each poll cycle in a broad `try/except`, logs the failure, and
lets the next scheduled poll retry — it does not propagate exceptions out of
the background task, so a transient 429 or network blip does not crash the
poller.

## 7. Timestamps & data-freshness notes

- All timestamps from the API are **Unix milliseconds** (some fields, like
  snapshot `updated`, are nanoseconds — check the specific field before
  dividing).
- During closed-market hours, `last_trade.price` reflects the last traded
  price and may include after-hours activity.
- The `day` object resets at market open; during pre-market it may still
  reflect the previous session until new trades post.

## 8. How this project uses it

`MassiveDataSource` (see `backend/app/market/massive_client.py`) polls
`get_snapshot_all()` once per interval for the full watchlist in a single
call, runs the synchronous client in a thread via `asyncio.to_thread` to
avoid blocking the event loop, and writes `last_trade.price` +
`last_trade.timestamp` into the shared `PriceCache`. This is selected by
`create_market_data_source()` (`backend/app/market/factory.py`) only when
`MASSIVE_API_KEY` is non-empty; see `planning/MARKET_DATA_API.md` for the
full interface this implementation conforms to.

## Sources

- [Polygon.io is Now Massive](https://massive.com/blog/polygon-is-now-massive)
- [massive-com/client-python README](https://github.com/massive-com/client-python/blob/master/README.md)
- [Full Market Snapshot | Stocks REST API](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Single Ticker Snapshot | Stocks REST API](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot)
- [Previous Day Bar (OHLC) | Stocks REST API](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [Custom Bars (OHLC) | Stocks REST API](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
- [What is the request limit for Massive's RESTful APIs?](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
- [Massive Pricing](https://massive.com/pricing)
