# FinAlly — the Finance Ally

A visually stunning, AI-powered trading workstation: live-streaming market data, a simulated portfolio, and an LLM chat assistant that can analyze your positions and execute trades on your behalf. Think Bloomberg terminal with an AI copilot.

This is the capstone project for an agentic AI coding course — built entirely by coding agents to demonstrate how orchestrated AI agents can produce a production-quality full-stack application. Agents coordinate through documents in [`planning/`](planning/), most importantly [`planning/PLAN.md`](planning/PLAN.md), the full project specification.

## Status

🚧 **In progress.** The market data subsystem is complete; the rest of the platform (API, database, frontend, LLM chat, Docker packaging) is still being built. See [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md) for what's done.

| Component | Status |
|---|---|
| Market data simulator (GBM, SSE-ready) | ✅ Complete |
| Massive (Polygon.io) live data client | ✅ Complete, unused by default |
| FastAPI app, database, portfolio/trade endpoints | ⏳ Not started |
| LLM chat assistant | ⏳ Not started |
| Angular frontend | ⏳ Not started |
| Docker packaging | ⏳ Not started |

## Vision

When finished, running a single command will open a browser to a live trading terminal: a watchlist of streaming prices, a $10,000 virtual cash balance, portfolio visualizations (heatmap, P&L chart, positions table), and a docked AI chat assistant that can analyze the portfolio and place trades through natural language. Full UX details are in [`planning/PLAN.md`](planning/PLAN.md) §2.

### Planned features

- **Live price streaming** — 10 default tickers (AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX) updating over SSE, with green/red flash animations on each tick
- **Sparklines** — mini price-history charts next to each watchlist ticker, accumulated client-side since page load
- **Buy/sell** — market orders only, instant fill at current price, no fees or confirmation dialogs
- **Portfolio heatmap** — treemap sized by position weight, colored by P&L
- **P&L chart** — total portfolio value over time
- **Positions table** — ticker, quantity, avg cost, current price, unrealized P&L, % change
- **AI chat assistant** — "FinAlly", backed by an LLM, that can analyze the portfolio and auto-execute trades or watchlist changes it recommends
- **Dark, data-dense terminal UI** — Bloomberg-inspired, accent yellow `#ecad0a`, blue `#209dd7`, purple `#753991`

### Planned API surface

| Method | Path | Description |
|---|---|---|
| GET | `/api/stream/prices` | SSE stream of live price updates |
| GET | `/api/portfolio` | Positions, cash balance, total value, unrealized P&L |
| POST | `/api/portfolio/trade` | Execute a trade: `{ticker, quantity, side}` |
| GET | `/api/portfolio/history` | Portfolio value snapshots (for the P&L chart) |
| GET | `/api/watchlist` | Current watchlist with latest prices |
| POST | `/api/watchlist` | Add a ticker: `{ticker}` |
| DELETE | `/api/watchlist/{ticker}` | Remove a ticker |
| POST | `/api/chat` | Send a chat message, get back a response plus any executed actions |
| GET | `/api/health` | Health check |

Full request/response contracts and the SQLite schema (`users_profile`, `watchlist`, `positions`, `trades`, `portfolio_snapshots`, `chat_messages`) are in [`planning/PLAN.md`](planning/PLAN.md) §7–8.

## Architecture (target)

Everything ships in a single Docker container on one port:

```
┌─────────────────────────────────────────────────┐
│  Docker Container (port 8000)                   │
│                                                   │
│  FastAPI (Python/uv)                             │
│  ├── /api/*          REST endpoints              │
│  ├── /api/stream/*   SSE streaming               │
│  └── /*              Static file serving         │
│                       (Angular build)             │
│                                                   │
│  SQLite database (volume-mounted)                │
│  Background task: market data polling/sim        │
└─────────────────────────────────────────────────┘
```

- **Frontend**: Angular + TypeScript, built to static assets and served by FastAPI
- **Backend**: FastAPI (Python), managed as a `uv` project
- **Database**: SQLite, lazily initialized, volume-mounted at `db/finally.db`
- **Real-time data**: Server-Sent Events (`/api/stream/prices`)
- **AI**: LiteLLM → OpenRouter, structured outputs for chat-driven trades
- **Market data**: simulator by default; real data via Massive API if `MASSIVE_API_KEY` is set

Full rationale for these choices is in [`planning/PLAN.md`](planning/PLAN.md) §3.

## What's built so far: market data

A self-contained market data subsystem lives in `backend/app/market/` — a `PriceCache`, a GBM-based simulator with correlated, per-sector price moves, a Massive/Polygon.io REST client behind the same interface, and an SSE stream factory. It's fully tested (79 tests, 99% coverage overall — every module is 94-100%, `stream.py` included) and has a standalone terminal demo:

```bash
cd backend
uv sync --dev
uv run market_data_demo.py
```

This runs a live Rich dashboard of all 10 default tickers with sparklines and an event log — no server, database, or frontend required. See [`backend/README.md`](backend/README.md) and [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md) for details.

Run the backend test suite:

```bash
cd backend
uv run pytest
```

## Environment Variables

Create a `.env` file at the project root:

```bash
# Required for AI chat once it's built
OPENROUTER_API_KEY=your-openrouter-api-key-here

# Optional: use real market data instead of the simulator
MASSIVE_API_KEY=

# Optional: deterministic mock LLM responses (for testing)
LLM_MOCK=false
```

## Project Layout

```
finally/
├── backend/           # FastAPI uv project (Python)
│   └── app/market/    # Market data subsystem (complete)
├── frontend/          # Angular project (not yet created)
├── planning/          # Shared spec and docs the agents build from
│   ├── PLAN.md
│   └── MARKET_DATA_SUMMARY.md
├── db/                # SQLite volume mount point (runtime)
└── test/              # Playwright E2E tests (not yet created)
```

See [`planning/PLAN.md`](planning/PLAN.md) §4 for the full target layout and the boundaries between components.

## Testing Strategy (planned)

- **Backend (pytest)** — market data math, trade execution and P&L edge cases, LLM structured-output parsing, API route contracts
- **Frontend (Jasmine/Karma via Angular CLI)** — component rendering, price flash animations, watchlist CRUD, chat rendering
- **E2E (Playwright, in `test/`)** — fresh-start flow, watchlist add/remove, buy/sell, portfolio visualizations, mocked AI chat, SSE reconnection. Runs against a container with `LLM_MOCK=true` for speed and determinism.

Full scenario list is in [`planning/PLAN.md`](planning/PLAN.md) §12.

## License

MIT — see [LICENSE](LICENSE).
