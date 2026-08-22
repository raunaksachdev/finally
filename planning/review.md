# Change Review — since last commit (`14550e1 Ready for Teams`)

Reviewed: 2026-08-22

## Scope

Nothing is staged or committed — this reviews the working tree only. No
application code (`backend/app/`, no `frontend/`) changed. The diff is
entirely documentation + new agent-orchestration tooling.

**Modified (tracked):**
- `planning/PLAN.md` (+50/-16)
- `README.md` (near-total rewrite)
- `.gitignore` (+5)
- `.claude/skills/cerebras/SKILL.md` (+2)

**Untracked (new):**
- `.claude-plugin/marketplace.json`
- `.claude/agents/reviewer.md`
- `.claude/commands/doc-review.md`
- `independent-reviewer/.claude-plugin/plugin.json`
- `independent-reviewer/hooks/hooks.json`
- `planning/review.md` (this file — regenerated each run, not itself a review subject)

Verified independently:
- `cd backend && uv run pytest -q` → **73 passed**.
- `cd backend && uv run pytest --cov=app --cov-report=term-missing -q` → **91% overall**. Note this is not evenly distributed: `app/market/stream.py` sits at **33%** coverage (lines 26-48, 62-87 uncovered) while everything else is 94-100%. Pre-existing, not touched by this diff, but worth flagging since the README leans on the aggregate "fully tested" framing.
- No `db/` directory exists in the current working tree or on `main`'s history. `db/.gitkeep` and `.env.example` *do* exist, but only on unrelated branches (`basic`, `codex`, etc.) that are **not ancestors of `main`** (confirmed via `git merge-base --is-ancestor`) — so on this branch's actual history, both files have genuinely never existed. Refining last review's phrasing: not "never existed in this repo" but "never existed on `main`."
- `LICENSE` exists at repo root, tracked since the initial commit (`8e9bb6b`).
- `.claude/settings.json` → `enabledPlugins` lists only `frontend-design`, `context7`, `playwright`. `independent-reviewer` is not in that list.

---

## `planning/PLAN.md`

Brings the spec in line with decisions apparently made elsewhere (Angular
instead of Next.js, a free-tier OpenRouter model instead of Cerebras, ECharts
instead of Lightweight Charts/Recharts) and adds several genuinely useful
clarifications: SQLite single-connection rationale, buy/sell `avg_cost` math,
SSE watchlist-change semantics, structured-output fallback strategy, and a
local-dev workflow subsection. No internal contradictions found — checked
that no stray Cerebras/`gpt-oss-120b` references remain anywhere in the file.

**Issue:**
- §4's directory listing still states `db/.gitkeep` "Directory exists in
  repo; finally.db is gitignored" — this is false on `main` (verified above).
  The new `.gitignore` rules added in this same diff (`db/*.db`,
  `!db/.gitkeep`) implicitly assume this file exists. Worth creating `db/` +
  `db/.gitkeep` in the same pass, since it's a one-line fix and the diff
  otherwise treats this as settled.

**Minor, non-blocking:**
- §9's model id `openrouter/nvidia/nemotron-3-ultra-550b-a55b:free` can't be
  verified from this repo — free-tier OpenRouter slugs rotate, worth a sanity
  check before backend LLM work starts.
- The new "single `execute_trade()`" constraint (§9, shared by
  `POST /api/portfolio/trade` and the chat action executor) is a good,
  testable rule — worth enforcing explicitly in code review once backend
  trade logic lands.

## `README.md`

Rewrite is a real improvement — the old version implied a working
`docker build`/`docker run` quick start that doesn't exist yet (no
Dockerfile, no `frontend/`). New version's status table, directory layout,
and commands all check out against actual repo state, with one exception:

**Issue:**
- "It's fully tested (73 tests, 84% coverage)" — the 73 is correct, but 84%
  is stale. Actual current coverage running the suite is **91%** overall,
  though that average masks `app/market/stream.py` at 33% (see Scope
  section). The 84% figure matches `planning/MARKET_DATA_SUMMARY.md` (line
  58), which appears to predate later test additions; the README borrowed a
  number that was already out of date. One-line fix, or drop the percentage
  and cite the test count alone to avoid future drift — and if precision
  matters, call out the `stream.py` gap rather than only the headline number.

No other issues found — `LICENSE` link is valid, layout matches actual
`backend/app/market/` structure, `uv sync --dev` / `uv run pytest` /
`uv run market_data_demo.py` commands match `backend/README.md`.

## `.gitignore`

```
db/*.db
db/*.db-journal
!db/.gitkeep
```
Correct and harmless in isolation (the negation pattern would preserve a
future `db/.gitkeep` while ignoring `db/finally.db`), but as noted above,
it's added to support a file/directory that doesn't exist yet on `main` — so
right now this rule has nothing to protect. Not wrong, just premature; tie it
to actually creating `db/.gitkeep`.

## `.claude/skills/cerebras/SKILL.md`

Adds a clear callout that this skill must not be used for FinAlly's chat
feature, pointing at PLAN.md §9. Good guardrail now that PLAN.md dropped
Cerebras as the mandated provider — prevents a future agent from following
stale skill guidance over current project spec. No issues.

---

## New agent tooling: `independent-reviewer` plugin, `.claude/agents/reviewer.md`, `.claude/commands/doc-review.md`

This is the meta-tooling that produces this very review. Findings for whoever
owns this setup:

1. **Plugin not enabled.** `independent-reviewer` (registered via
   `.claude-plugin/marketplace.json`) is absent from
   `.claude/settings.json`'s `enabledPlugins`. If plugin hooks only fire for
   enabled plugins, the Stop hook in `independent-reviewer/hooks/hooks.json`
   won't run automatically until it's added there. Confirm this is
   intentional staging rather than an oversight.
2. **Plugin isn't self-contained.** `independent-reviewer/` only has
   `.claude-plugin/plugin.json` and `hooks/hooks.json` — no `agents/` or
   `commands/` of its own. The actual `change-reviewer` agent lives at the
   project level (`.claude/agents/reviewer.md`), and the hook depends on it
   being present there. Enabling the plugin alone in a different repo (or
   without these project-level files) would leave the hook invoking an agent
   that doesn't exist. Consider moving the agent definition inside the
   plugin directory if portability matters.
3. **Recursion guard looks correct.** The hook checks
   `[ -z "$CLAUDE_STOP_HOOK_ACTIVE" ]` before invoking
   `claude --agent change-reviewer` and sets the env var when it does, which
   should prevent the reviewer's own Stop event from re-triggering itself.
4. **Naming mismatch (cosmetic).** Hook invokes `--agent change-reviewer`;
   the agent is defined in a file named `reviewer.md` (not
   `change-reviewer.md`) — wired correctly via the `name:` frontmatter field,
   but easy to trip over when grepping for the agent by filename.
5. **`doc-review` slash command's relationship to this plugin is unclear.**
   `.claude/commands/doc-review.md` reviews a single planning doc in place
   (adds a feedback section) — a different workflow from `change-reviewer`'s
   "review everything since last commit → `planning/review.md`" job. Both
   were added in the same diff; worth confirming these are two intentionally
   separate tools rather than one meant to replace the other.
6. **Description text is duplicated across three files** (agent frontmatter,
   `marketplace.json`, `plugin.json`), all saying essentially "review changes
   since last commit." Not a bug, just three places to keep in sync if scope
   changes later.
7. **`planning/review.md` is self-overwriting output, not reviewable input.**
   Since this file is regenerated by the very agent this review evaluates,
   treat its own historical diffs as tooling exhaust rather than content to
   scrutinize — noted here only so a future run doesn't waste effort
   re-reviewing its own prior output.

None of these block anything — this tooling is additive and doesn't touch
the application.

---

## Overall

No application code changed. Documentation changes are accurate and
internally consistent, with two small, non-blocking staleness issues:

1. README's "84% coverage" is stale (actual: 91% overall, with a real
   coverage gap in `app/market/stream.py` at 33% that the headline number
   obscures; test count of 73 is correct).
2. `.gitignore`'s new `!db/.gitkeep` rule, and PLAN.md §4's claim that
   `db/.gitkeep` already exists in the repo, both refer to a file that has
   never existed on `main` (it exists only on unrelated, non-ancestor
   branches) — a pre-existing gap this diff doesn't introduce but also
   doesn't close, despite touching the adjacent `.gitignore` rule in the
   same commit.

The new `independent-reviewer` plugin/agent tooling is functionally sound
(correct recursion guard, correctly wired agent name) but is not yet enabled
in `.claude/settings.json` and is not fully self-contained within the plugin
directory.
