# feature-planning

Plan a new feature before any code is written. Apply when the user says
"plan a feature", "let's design <X>", "before we code <Y>", "give me a
spec for", "preflight <Z>", or any other ask that wants exploration + a
written plan before implementation. Read `README.md` and the workspace
`Cargo.toml` `[workspace.lints]` first — forge has no `AGENTS.md`; every
plan must respect the crate layering (`forge-jobs` → `forge-jobs-api` →
`forge-jobs-ui`; `forge-charts` standalone) and the
commit-only-on-explicit-go rule.

The output is a self-contained markdown spec at `docs/plans/<slug>.md`
that another instance of Claude (or a human) can execute cold. The spec
is finished only after a **red-flag pass** and the **commit-stage
breakdown** are filled in. No code is written by this skill — it stops
with the spec on disk and waits for the user's "go."

This skill is question-driven on purpose. Default to asking, not
guessing. The bar is: an engineer who never opened the repo could follow
the spec end-to-end and ship the feature without re-deriving anything.

## Workflow

```
Phase 1: First-pass interview        (AskUserQuestion, in batches)
Phase 2: Explore the repo            (read files, never edit)
Phase 3: Draft the spec              (write docs/plans/<slug>.md)
Phase 4: Red-flag pass               (re-interview the gaps)
Phase 5: Vulnerabilities catalogue   (per topic)
Phase 6: Commit-stage breakdown      (one commit per stage)
Phase 7: Stop and wait for "go"
```

Each phase has a stop condition. Do not skip ahead.

## Phase 1 — First-pass interview

Use the `AskUserQuestion` tool in batches of 1–4 questions. Ask
everything you don't already know with high confidence. Better to ask one
extra question than to assume.

Cover at least these buckets — ask every one that isn't trivially
answered by the user's opening sentence:

1. **Goal & success criteria.** What is the user-visible outcome? What
   does "done" look like — a passing test, a working panel button, an
   observable queue behavior? If the user can't name a check, push for
   one.
2. **Scope boundary.** What is explicitly **out of scope**? Which things
   look related but should *not* be touched in this PR? If the user says
   "everything related to X," push back — a feature is one logical
   change.
3. **Layer fit (forge-specific).** Which crates does this touch
   (`forge-jobs` runtime/storage / `forge-jobs-api` transport /
   `forge-jobs-ui` panel / `forge-charts`)? Does it touch the storage
   trait — and therefore **both** the SQLite and Postgres adapters?
4. **Data shape.** What's the new (or changed) shape in: the storage
   schema (and the matching migration in *both* adapters), the trait
   method signatures, the wire/IPC DTO in `forge-jobs-api`, the panel
   state in `forge-jobs-ui`? Each layer that changes shape is its own
   coordination risk.
5. **Reuse.** What existing code should be extended rather than forked?
   The storage trait, the `QueueIpc` trait, the runtime loop, the
   `RefreshTick` / poll-interval context, the in-DOM `Confirmer`, an
   existing `forge-charts` series type — name what's already there.
6. **External surface.** Does this change anything callers see — HTTP
   routes / IPC commands, DTO field names, the `QueueIpc` trait, the
   published `forge-jobs` / `forge-charts` API? A change to any of these
   is a SemVer question (these crates publish to crates.io).
7. **Cross-replica / concurrency budget.** If this lands on the queue
   runtime, what's the behavior with N replicas sharing one Postgres
   queue? Does it need a lock, a `NOTIFY`, an idempotency key? "Single
   SQLite process only" is a valid answer — but say it.
8. **Failure mode.** What happens when the dependency is down / slow /
   returns garbage? DB unreachable, a worker dies mid-job, a payload is
   malformed, the rate limit is hit. A feature without an articulated
   failure mode is half-designed.
9. **Rollback.** How do we turn this off if it misbehaves? A feature
   flag, a Cargo feature, dropping a route, reverting one commit? If it
   can't be turned off, the design needs to be smaller.
10. **Timeline / urgency.** Is this blocking something else?

Phrase questions so the user can answer in one short reply each. Use the
`multiSelect` form when the choices are bounded.

**Stop condition.** Every bucket either has an answer or has been
deliberately marked "not applicable" with a one-sentence reason.

## Phase 2 — Explore the repo

Read the relevant files. Do not edit. The goal is to populate the spec's
"Files to modify / create / delete" and "Reusable existing code" sections
with concrete paths and symbol names, not guesses.

Practical pointers (pick what's relevant):

- `tree -I 'target|*.lock' crates/` for a layout overview.
- `grep -rn '<symbol>' crates/<crate>/src` for any name the user
  mentioned.
- Read the top-level `lib.rs` of each crate you'll touch.
- For storage changes, read **both** `storage/sqlite/` and
  `storage/postgres/` so the spec names the parity work explicitly.
- For UI changes, read `queue_root.rs` (the panel root + shared context)
  and the relevant tab module.

**Stop condition.** You can name, for every file in the upcoming spec,
the function or struct you'll change and why. If you can't, ask the user
one more question or read one more file — don't extrapolate.

## Phase 3 — Draft the spec

Write `docs/plans/<slug>.md` using the template below. The slug is short
kebab-case (`queue-priority-lanes`, not
`add_the_priority_lanes_feature_v2`). Mark every section present even if
empty — `None` is a valid value; *missing* is not.

````markdown
# Plan: <Feature title>

**Status:** draft (awaiting confirmation)
**Slug:** <slug>
**Author/agent:** <session>
**Date:** <YYYY-MM-DD>

## 1. Goal

One sentence on what the user can do after this ships that they can't
today. One sentence on how we'll know it works (the check).

## 2. Why now

Why the cost of building this is justified now. If there's no "why now,"
ask the user.

## 3. Non-goals (out of scope)

Bulleted list of things this PR will not touch. Load-bearing: it's how
the reviewer judges scope creep.

## 4. Affected crates

For each crate, one line on what changes there. Crates not touched are
listed as `(unchanged)` so the omission is deliberate.

- `forge-jobs` (runtime / storage): …
- `forge-jobs-api` (transport): …
- `forge-jobs-ui` (panel): …
- `forge-charts`: …

## 5. Files

### To modify
- `crates/<crate>/src/<file>.rs` — what changes and why.

### To create
- `crates/<crate>/src/<file>.rs` — purpose, public API.

### To delete
- `crates/<crate>/src/<file>.rs` — why it's safe to remove.

(If a section has no entries, write `None`.)

## 6. Reusable existing code

Concrete names: the storage trait + its two adapters, `QueueIpc`, the
runtime loop, `RefreshTick` / `PollIntervalMs` context, `Confirmer`, a
`forge-charts` series type. If you can't name three reusable things, you
didn't read enough yet — go back to Phase 2.

## 7. Data & wire shapes

- **Storage schema** — new tables/columns and the migration, **mirrored
  in both the SQLite and Postgres adapters**; note any type/SQL dialect
  difference.
- **Storage / `QueueIpc` trait** — new or changed method signatures.
- **HTTP / IPC DTOs** (`forge-jobs-api`) — new fields, `#[serde]` attrs,
  breaking vs additive.
- **Panel state** (`forge-jobs-ui`) — new signals/context, polling
  implications.

## 8. Test plan

What proves it works:
- The failing unit test we write first (the TDD seed).
- Adapter-parity tests (SQLite + Postgres) where storage changes.
- `cargo test --workspace`; wasm `clippy --target
  wasm32-unknown-unknown` for UI changes.
- Manual / `curl` recipe for the API surface, or a panel click-path, if
  any.

## 9. Risks (filled by Phase 4 red-flag pass)

(See §11 for the catalogue; cross-reference here.)

## 10. Commit-stage breakdown (filled by Phase 6)

| # | Stage | Crate(s) | Verification |
|---|-------|----------|--------------|
| 1 | …     | …        | …            |

## 11. Vulnerabilities & open questions

Tracked per category — see Phase 5. Anything still unanswered ends the
spec in the **Open questions** list, not silently dropped.
````

**Stop condition.** Every section is present. The reader can name every
file that will change.

## Phase 4 — Red-flag pass

Now re-interview the user with the questions you didn't think to ask the
first time. This is the highest-leverage phase. Don't skip it because the
spec "looks complete."

Walk the catalogue below. For each entry that *could* apply, either
confirm it doesn't, or convert it into a concrete question for the user
(via `AskUserQuestion`). Add answers back into the spec — under §9 Risks,
§11 Vulnerabilities, or §3 Non-goals as fits.

### Red-flag catalogue

- **Concurrency.** Two workers claiming one job, SELECT-then-act windows,
  last-writer-wins `UPDATE`s, a counter read-modify-write race.
- **Failover.** What if a worker dies mid-job? Does the row requeue, or
  sit `in_progress` forever?
- **Idempotency.** If the job retries (or a `NOTIFY` is delivered twice),
  does the side effect fire twice?
- **Clock skew.** DB `now()` vs the runtime's `chrono::Utc::now()` — which
  decides `scheduled_at` / `throttled_until` / retry timing?
- **Adapter parity.** Does the change behave identically in SQLite and
  Postgres? Dialect differences (types, `RETURNING`, `ON CONFLICT`,
  `SKIP LOCKED`) are where parity quietly breaks.
- **Resource exhaustion.** Memory/row cost at the largest realistic queue
  depth. Can a malformed payload cost O(n²)?
- **Backwards compatibility.** Old DB rows, old DTOs, a mixed-version
  replica fleet during a rolling deploy. SemVer for the published crates.
- **Migration / rollout.** Schema migration order; is it reversible; does
  it require a coordinated deploy?
- **Observability.** If this misbehaves in production, what
  log / metric / span tells us? If "we'd have no idea," fix that now.
- **Security & secrets.** New unauthenticated API surface? A payload or
  connection string logged or rendered in the panel? Unescaped identifier
  reaching SQL/`NOTIFY`?
- **Reversibility.** If we ship and it's wrong, what's the rollback?
- **Convention drift.** Does this break a workspace lint or the layering?
  If a lint must be relaxed, that's a deliberate, documented decision, not
  a quiet `#[allow]`.

After this pass, also pose two pre-mortem prompts to the user explicitly:

1. **"Imagine this feature has shipped and is causing an incident. What's
   the most likely root cause?"**
2. **"What's the thing about this design you're least sure about?"**

Capture the answers verbatim into §9 Risks. They're usually the most
honest signal in the whole spec.

**Stop condition.** Every catalogue entry is either confirmed "doesn't
apply" (with a one-line reason) or has a row in §9 Risks with a concrete
mitigation or follow-up question.

## Phase 5 — Vulnerabilities catalogue (per topic)

Highlight, in §11 of the spec, every vulnerability that fits this
feature's topic. Don't dump the whole list — pick the ones that actually
bite. For each, write:

```
- <ID>  <category>  <one-line description>
        Where it bites: <crate::module::function>
        Mitigation: <concrete change or test>
        Owner / open question: <name or "?">
```

Categories to scan against:

- **Correctness** — a job run twice or lost, a wrong retry/backoff
  schedule, a throttle counter that over- or under-counts.
- **Concurrency** — two workers on one row, dispatch without `SKIP
  LOCKED`, a cron double-fire, cancellation lost across replicas.
- **Security** — unauthenticated mutate/purge route, payload/secret leak
  in logs or the panel DOM, unescaped identifier into SQL/`NOTIFY`,
  deserialization of attacker-controlled payload bytes.
- **Performance** — per-dispatch allocation, a full table scan where an
  index is needed, a busy-poll where `NOTIFY` would do, panel re-render
  storms.
- **Data integrity** — schema drift between the two adapters, a partial
  migration, a `NOTIFY` delivered before the committing transaction is
  visible.
- **Operational** — what alerts fire, what the rollback diff looks like.

Drop the catalogue rows into §11 of the spec with their concrete
mitigations.

## Phase 6 — Commit-stage breakdown

Fill in §10 of the spec. The rules:

- **Each commit is one logical change** that compiles and passes
  `fmt + clippy + test` on its own.
- **Cross-crate features are at least two commits**: the `forge-jobs`
  storage/runtime change with its test lands first; the API/UI wiring
  lands after.
- **Storage changes touch both adapters in the same commit** (or an
  explicitly-sequenced pair) so the tree never compiles with one backend
  ahead of the other.
- **Each commit has a verification line** — the test or command that
  proves it. "It compiles" is not verification.
- **A migration is its own commit**, not bundled with the feature logic.
- **The last commit is verification glue** — the integration test or curl
  recipe — if it doesn't already fit in an earlier commit.

Template row:

| # | Stage | Crate(s) | Verification |
|---|-------|----------|--------------|
| 1 | Add `<col>` + migration to both adapters, with parity test | `forge-jobs` | `cargo test -p forge-jobs` |
| 2 | New storage-trait method + impls | `forge-jobs` | unit test both adapters |
| 3 | API handler returns `<thing>` in DTO | `forge-jobs-api` | handler test |
| 4 | Panel surfaces it | `forge-jobs-ui` | `clippy --target wasm32-unknown-unknown` + manual click |

Number commits in the order they should land. If any two could land in
either order, say so.

## Phase 7 — Stop and wait

Output:

1. The path to the spec on disk (`docs/plans/<slug>.md`).
2. A 5-bullet summary: goal, scope, crates touched, top risk, number of
   commit stages.
3. Any **Open questions** that didn't get answered in Phase 1 or 4.
4. The literal phrase: *"Spec is ready. Reply 'go' to start implementing,
   or tell me what to change."*

Do not start implementing. Do not stage. Do not commit. This is the end
of the planning workflow; execution is a separate session driven by the
spec.

## Anti-patterns

- **Writing the spec from the user's one-sentence ask.** Without Phase 1,
  the spec encodes assumptions instead of decisions.
- **Skipping Phase 4 because the first draft "looked complete."** That's
  exactly when the red-flag pass earns its keep.
- **Spec with no verification per commit.** "I'll figure it out when I
  implement" means the implementer figures it out under pressure.
- **A storage change that lands in one adapter without the other.** The
  tree must never compile/behave with SQLite and Postgres out of sync.
- **One mega-commit that touches three crates.** Split it.
- **Listing risks without mitigations.** A risk with no row in §11 or §10
  is decoration.
- **Burying the rollback story.** If the spec has no §3 entry on "how do
  we turn this off," the design is incomplete.

## When to stop

The spec exists at `docs/plans/<slug>.md`, every section is filled, the
red-flag pass has been done, the commit-stage table reflects a real
implementation order, and the user has the spec path + a short summary in
hand. The next action is theirs.
