# Market Data Backend — Code Review

**Date:** 2026-08-25
**Scope:** `backend/app/market/` (9 source files) and `backend/tests/market/` (6 test files, 73 tests)
**Reviewer note:** This supersedes `planning/archive/MARKET_DATA_REVIEW.md` (2026-02-10). All "must fix" and "should fix" items from that earlier review have been verified as resolved (see §4). This is a fresh, independent pass looking for anything new.

---

## 1. Test Results

Ran from a clean `uv sync --extra dev`:

```
uv run pytest -v            → 73 passed, 0 failed
uv run pytest --cov=app     → 91% overall
uv run ruff check app/ tests/  → All checks passed
uv run ruff format --check  → 3 test files would be reformatted (cosmetic only, see §3.4)
```

**Coverage by module:**

| Module | Coverage | Missing |
|---|---|---|
| `models.py` | 100% | — |
| `cache.py` | 100% | — |
| `interface.py` | 100% | — |
| `factory.py` | 100% | — |
| `seed_prices.py` | 100% | — |
| `__init__.py` | 100% | — |
| `simulator.py` | 98% | L149 (duplicate-add guard), L268-269 (exception-handler branch in `_run_loop`) |
| `massive_client.py` | 94% | L85-87 (`_poll_loop` body), L125 (`_fetch_snapshots` real body) |
| `stream.py` | 33% | L26-48, 62-87 — essentially the whole SSE generator and route handler |
| **Total** | **91%** | |

This is a real improvement over what `planning/MARKET_DATA_SUMMARY.md` documents (see §5.1 — that doc is now stale). With the `massive` package actually installed, `massive_client.py` tests exercise real code paths and coverage rose from the previously-recorded 56% to 94%.

`stream.py` at 33% remains the one genuine, unaddressed gap — no test exercises `_generate_events` end-to-end. This was flagged in the prior review and in `planning/MARKET_DATA_DESIGN.md` §12.4 (which even supplies the `httpx.ASGITransport` recipe to close it) but the test still doesn't exist. Since this endpoint is the only consumer-facing piece of the whole subsystem, it's worth adding before this is called fully tested.

---

## 2. Architecture Assessment

Confirmed by reading all 9 source modules end-to-end: the design holds up. Strategy pattern (`MarketDataSource` ABC with `SimulatorDataSource`/`MassiveDataSource`), single-writer `PriceCache` as the only read path for downstream consumers, and a factory that's the sole place branching on `MASSIVE_API_KEY` — all exactly as documented in `planning/MARKET_DATA_API.md` and `planning/MARKET_DATA_DESIGN.md`, and the docs' code samples match the real source verbatim (no drift found).

Strengths worth calling out:
- `PriceUpdate` (`frozen=True, slots=True`) makes `direction`/`change`/`change_percent` computed properties instead of stored fields — they can't drift out of sync with `price`/`previous_price`.
- Both background loops (`SimulatorDataSource._run_loop`, `MassiveDataSource._poll_loop`) wrap their step logic in try/except so one bad tick/poll can't kill the task.
- `add_ticker` seeds the cache synchronously before returning (simulator) so a freshly-added ticker is never blank for up to 500ms — a real UX detail, correctly implemented.
- GBM math is textbook-correct: `S(t+dt) = S(t) * exp((mu - sigma²/2)*dt + sigma*sqrt(dt)*Z)`, with `dt` correctly derived from a 500ms tick against a 252-day/6.5h trading year.

### 2.1 Verified: Cholesky correlation matrix never fails for the actual ticker universe

The correlation-matrix construction in `_rebuild_cholesky`/`_pairwise_correlation` assigns different fixed correlations depending on pair type (intra-tech 0.6, intra-finance 0.5, TSLA-anything 0.3, cross-sector/unknown 0.3). This kind of ad-hoc, non-block-consistent correlation assignment isn't *mathematically* guaranteed to produce a positive-semi-definite matrix in general (which `np.linalg.cholesky` requires), so I stress-tested it directly:

```python
# All subsets (size 2..12) of {7 tech tickers, 2 finance tickers, TSLA, 3 unknown tickers}
# → 4,083 combinations tested, 0 Cholesky failures
```

For the actual ticker set this project uses (the 10 defaults plus arbitrary user-added symbols, which all fall into the "unknown/cross-sector" 0.3 bucket), this is safe. Flagging only so it's understood as empirically-verified-safe rather than mathematically-guaranteed-safe — if the correlation constants in `seed_prices.py` are ever tuned upward (e.g., intra-tech pushed to 0.9+ while cross-sector stays low), it would be worth re-running a check like this before shipping the change, since a `LinAlgError` there would crash `add_ticker`/`remove_ticker` (and thus a live watchlist mutation) with no handling for it anywhere in the call chain.

---

## 3. New Findings (not in the prior review)

### 3.1 `PriceCache.update()`: a timestamp of exactly `0.0` is silently replaced (Severity: Low)

```python
ts = timestamp or time.time()
```

`0.0` is falsy in Python, so a caller that explicitly passes `timestamp=0.0` gets `time.time()` instead — confirmed by direct test:

```
cache.update('AAPL', 190.0, timestamp=0.0)  →  update.timestamp == time.time(), not 0.0
```

In practice this can't currently be hit by production code paths (`MassiveDataSource` converts real millisecond epoch timestamps, which are never 0; the simulator never passes `timestamp` at all), so this is dormant rather than an active bug. The fix, if it's worth making, is `ts = timestamp if timestamp is not None else time.time()`.

### 3.2 `test_exception_resilience` doesn't actually test exception resilience (Severity: Low, test-quality)

`tests/market/test_simulator_source.py::test_exception_resilience` starts a normal simulator, sleeps, and asserts the background task is still running — it never injects a failure into `GBMSimulator.step()`. This is consistent with the coverage report: `simulator.py` L268-269, the `except Exception: logger.exception(...)` branch inside `_run_loop`, is genuinely uncovered. The resilience behavior is real (verified by reading the code — a bare `try/except Exception` around the step call, which does what's claimed) but the test doesn't exercise the failure path it's named for. A tightened version would monkeypatch `sim.step` to raise once and assert the loop survives and later ticks still land.

### 3.3 `test_custom_update_interval` is timing-dependent and could flake under load (Severity: Low, test-quality)

```python
source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
await source.start(["AAPL"])
initial_version = cache.version
await asyncio.sleep(0.05)  # Should get ~5 updates
assert cache.version > initial_version + 2
```

This assumes at least 3 ticks land inside a 50ms window against a 10ms interval. On a loaded CI runner or under GIL contention from other tests running in parallel, this margin is thin enough to occasionally fail without any actual regression. Not currently flaky in this environment (ran the suite multiple times without failure), but worth a wider margin (e.g., `update_interval=0.02`, `sleep(0.15)`, assert `> initial_version`) if it's ever seen to flake in CI.

### 3.4 Three test files are not `ruff format`-clean (Severity: Trivial)

`ruff check` (the linter) passes clean, but `ruff format --check` flags `test_models.py`, `test_simulator.py`, and `test_simulator_source.py` — a handful of `PriceUpdate(...)` constructor calls exceed the 88-char wrap width ruff's formatter prefers, even though the project's own `line-length = 100` / `ignore = ["E501"]` lint config doesn't care. Cosmetic only; `uv run ruff format app/ tests/` would silently fix it. Not worth blocking on, but noting since "clean lint" and "clean format" aren't the same claim.

---

## 4. Status of the Prior Review's Findings — all resolved, verified against current source

| # | Prior finding | Verified status |
|---|---|---|
| 1 | Missing `[tool.hatch.build.targets.wheel]` in `pyproject.toml` — broke `uv sync`/Docker builds | **Fixed.** Present in `pyproject.toml`; `uv sync --extra dev` succeeds cleanly. |
| 2 | `massive` lazy-imported inside methods, breaking `patch("...RESTClient")` | **Fixed.** `massive_client.py` imports `RESTClient`/`SnapshotMarketType` at module level (lines 8-9). |
| 3 | `_generate_events` annotated `-> None` despite being an async generator | **Fixed.** Now `-> AsyncGenerator[str, None]` (`stream.py:55`). |
| 4 | `SimulatorDataSource.get_tickers()` reached into `GBMSimulator._tickers` (private) | **Fixed.** `GBMSimulator.get_tickers()` is now a public method; the adapter delegates to it (`simulator.py:140-142`, `257-258`). |
| 5 | Unused `DEFAULT_CORR` constant, confusingly separate from `CROSS_GROUP_CORR` | **Fixed.** `seed_prices.py` only defines `CROSS_GROUP_CORR`; no `DEFAULT_CORR` remains. |
| 6 | Unused imports (`pytest`, `math`, `asyncio`) in 4 test files | **Fixed.** `ruff check` reports zero warnings across `app/` and `tests/`. |
| 7 | 5 `test_massive.py` tests failed without the `massive` package installed | **Fixed.** All 73 tests pass with `massive` installed as a real dependency (declared in `pyproject.toml`, confirmed via `uv sync`); mocks now use `patch.object(source, "_fetch_snapshots", ...)` and direct `source._client = MagicMock()` assignment rather than patching a module-level name that didn't exist. |

No regressions were introduced while fixing these — all corresponding tests still pass and the surrounding code reads cleanly.

---

## 5. Documentation Accuracy

### 5.1 `planning/MARKET_DATA_SUMMARY.md` is stale (Severity: Low, docs-only)

This file (last describing itself as the authoritative summary) states:
- "84% overall coverage" — actual is **91%**.
- `massive_client.py`: "56% (expected — API methods mocked)" — actual is **94%**, because `massive` is now a real installed dependency and the module-level-import fix (§4, item 2) means tests actually exercise real code paths instead of failing before they get there.

Both numbers were accurate as of the file's writing but predate the fixes in §4. The top-level `README.md` (`"73 tests, 91% coverage overall — stream.py is the weak spot at 33%..."`) already has the correct, current numbers — `planning/MARKET_DATA_SUMMARY.md` is the one document left with the old figures. Worth a one-line update so a future reader doesn't cite the wrong number.

### 5.2 Everything else checked out

- `planning/MARKET_DATA_API.md` and `planning/MARKET_DATA_DESIGN.md` code samples were diffed by eye against the real source files in `backend/app/market/` — no drift found; both documents accurately describe the shipped implementation, including the specific fixes from §4 (they explicitly call out that they supersede `planning/archive/MARKET_DATA_DESIGN.md` for exactly this reason).
- `planning/MASSIVE_API.md` accurately describes the `MassiveDataSource` implementation and correctly scopes itself as reference-only per `CLAUDE.md`/`PLAN.md` §6 (simulator is the supported path; `MASSIVE_API_KEY` stays unset).
- `backend/CLAUDE.md` and `backend/README.md` match the actual public API surface (`app.market` re-exports) and test/lint commands — both run clean as documented.

---

## 6. Verdict

The market data subsystem remains solid, well-tested, and accurately documented (with the one stale-numbers exception in §5.1). All issues from the prior review are genuinely fixed, not just marked fixed. Nothing found in this pass rises above "low severity" / "nice to have":

**Worth doing, not urgent:**
1. Add an ASGI-level integration test for `stream.py` (recipe already exists in `planning/MARKET_DATA_DESIGN.md` §12.4) — this is the one real, still-open coverage gap, and it covers the subsystem's only externally-facing endpoint.
2. Update the stale coverage figures in `planning/MARKET_DATA_SUMMARY.md` (§5.1).

**Nice to have:**
3. Fix the falsy-`0.0`-timestamp edge case in `PriceCache.update()` (§3.1) — dormant today, cheap one-line fix (`if timestamp is not None else`).
4. Make `test_exception_resilience` actually inject a failure (§3.2).
5. Widen the timing margin in `test_custom_update_interval` to remove latent flakiness risk (§3.3).
6. Run `ruff format` on the 3 flagged test files (§3.4).

No blockers. This module is ready to be built on by the rest of the platform as-is.
