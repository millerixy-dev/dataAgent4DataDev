# CLAUDE.md

Operating notes for AI assistants working in this repo. Optimized for the
shape of *this* project, not generic advice.

## What this is

Data-development platform on top of CDP 7.1 + DolphinScheduler 3.1.7 +
Spark 2.4.7. We are mid-build: groups 1–5 of the OpenSpec MVP are landed
(driver renderer, spark_submit shim, Variable Resolver, Command Generator,
Snapshot Service); groups 6–13 (Publish, DS Adapter, Instance Service,
Frontend, …) are not. Read `openspec/changes/data-dev-platform/tasks.md`
for the live checklist before assuming any module exists.

## Hard rules

1. **Use `uv`. Never `pip`.** Run `uv sync --group dev`, `uv run pytest`,
   `uv run ruff check .`. The lock file `uv.lock` ships in git; CI will
   `uv lock --check`.
2. **Don't add `pyspark` to dev deps.** The driver imports `pyspark` only
   inside `main()` so tests never need a Spark install. If a test needs
   SparkSession, gate it `pytest.mark.skipif(no pyspark)` and document why.
3. **OpenSpec is the source of truth.** Behaviour changes go through
   `openspec/changes/data-dev-platform/specs/<capability>/spec.md` first
   (`WHEN/THEN` scenario), then code. Run `openspec validate data-dev-platform`
   after every spec edit.
4. **TDD, properly.** Red → verify failure → green → refactor. Don't write
   implementation before a failing test. If you genuinely cannot watch a test
   fail because the implementation already covers it, say so explicitly in
   your reply rather than skipping the protocol silently.
5. **Conventional Commits.** `feat(scope): …`, `refactor(scope): …`,
   `chore(scope): …`. Scopes used so far: `driver`, `contracts`, `shell`,
   `resolver`, `command-generator`, `openspec`, `uv`. Reuse them.
6. **Don't commit secrets, virtualenvs, caches, or `.claude/`.**
   `.gitignore` already covers them; if you add tooling state, extend it.
7. **Compose tests must SKIP, not FAIL, when the stack is down.** Tag them
   `@pytest.mark.compose` and probe the daemon with `docker inspect` (TCP
   probes lie on macOS / Docker Desktop). `uv run pytest` with no docker
   running is the canonical green-state baseline.

## Architectural invariants — never violate

These are the load-bearing decisions from `design.md`. If a refactor pressures
any of them, stop and surface the conflict instead of bending the invariant.

- **DS does not own user material.** No user SQL, no driver script in DS
  resource center. Everything in HDFS snapshots addressed by
  `(env, task_id, version_id)`.
- **Snapshots are immutable.** Append a new `version_id`; never overwrite or
  delete. Rollback = pointer flip.
- **Instance triple `(task_id, version_id, biz_date)` locks at creation.**
  Retries reuse the triple, `retry_count++`. Backfills may pick the version
  explicitly via UI but still lock the triple.
- **Backend is the only spark-submit writer.** DS shell body = `eval
  "$SPARK_CMD"` plus kinit / appId grep / callback. No conf assembly on the
  worker.
- **Driver stays backwards compatible.** A driver upgrade must not break any
  historical snapshot. The legacy `--sql-file --biz-date` invocation must
  always work.
- **Project variables (`${prj.X}`) bake at publish; runtime variables
  (`${dt}`, `${dt-N}`, `${hr}`) render at driver.** Never mix the layers.
- **`prj.` is the only project-variable namespace.** Hyphens, dots without
  prefix, etc., are unresolved → publish fails.
- **Command generator: blacklist > whitelist.** Adding a key to the whitelist
  is a change-control event (RFC + spec update). Adding to the blacklist is a
  hotfix. Never disable the `shlex.split` self-check.

## Working layout

```
contracts/runtime_variables.yaml    Cross-stack variable contract (driver /
                                    Resolver / frontend all read this).
                                    Editing this file requires updating tests
                                    in test_variable_catalog.py and
                                    test_renderer_*.py the SAME commit.

pyspark_driver_pkg/                 Pyspark-free Python modules. Safe to import
                                    in tests.
  variable_catalog.py                Loads YAML; emits VariableCatalog.
  renderer.py                        Pure render. Six derive functions are
                                     whitelisted; do NOT add eval-style
                                     dispatch.
  sql_splitter.py                    Lifted from legacy driver, leave alone
                                     unless you have a regression case.
  driver.py                          run_render() — the testable half of main.
  resolver.py                        bake() / preview(); InMemory store stub.
  command_generator.py               Backend command builder. Security-critical.

pyspark_driver.py                   YARN entry. Late-imports pyspark inside
                                    main(). Keep imports cheap so dry-run works
                                    without Spark.

spark_submit.sh                     DS Worker entry. Five jobs: validate env,
                                    kinit, parameter-expand ${biz_date}, eval,
                                    grep appId, POST callback. Don't grow it.

tests/                              pytest. 100 cases.
  test_cli_e2e.py                    Two end-to-end runs stitching the chain
                                     Resolver → snapshot file → shell → driver
                                     --dry-run. Keep at most one of these per
                                     "vertical slice" landing.
```

## Local docker stack (A-line)

`compose/` ships a minimal stack — Hive Metastore + Postgres + (opt-in)
DolphinScheduler standalone. **No HDFS, no YARN, no Kerberos.** Use it
when you need a real HMS to validate publish-time wiring (Snapshot
Service, future Publish Orchestrator, future DS Adapter).

```bash
make up           # HMS + Postgres
make up-ds        # plus DolphinScheduler
make ps           # status
make test-compose # pytest -m compose against the running stack
make down         # stop (keeps volumes)
make clean        # drop volumes too
```

When the stack is down, the six `@pytest.mark.compose` tests SKIP, so
`uv run pytest` is always green. Default ports are intentionally
non-standard (15433, 19083, 10012) so this stack coexists with another
CDP-flavoured docker stack on the same machine.

The Kerberos-on-YARN B-line stack does NOT exist yet. If a task in
`tasks.md` requires real WebHDFS / SPNEGO / yarn-cluster mode, surface
that the A-line cannot satisfy it rather than half-building B-line.

## Adding a new capability sliceThe repeated pattern, demonstrated in commits `bed9d82..c437935`:

1. Read `openspec/changes/data-dev-platform/specs/<capability>/spec.md`. If
   it doesn't capture what you're about to do, update it first.
2. Write the failing test in `tests/test_<module>.py`. Verify it fails for
   "feature missing", not "import error".
3. Stub the module with `NotImplementedError` so collection passes; re-run
   tests to confirm shape of failure.
4. Implement minimally to green.
5. Add edge-case tests (the boundaries `${dt-0}`, `${dt-N}` overflow, hostile
   shell metachars, etc.).
6. Tick the matching line in `tasks.md`.
7. `uv run pytest` must be 100% green; `uv run ruff check .` must be clean.
8. Commit. The commit message lists every test added with a short note on
   what it covers — `git log` becomes the design diary.

## Things that look wrong but aren't

- `--trace-id "${TRACE_ID}"` in the generated command isn't quoted via
  `shlex.quote`. **By design**: that's a shell variable reference set by the
  platform via DS env vars, not user input. `shlex.split` parses it as a
  literal token; the shell expands it at `eval` time.
- The driver renders `${dt-N}` even when `N=0`. Allowed; semantically equal
  to `${dt}`. Not worth blocking.
- Commit 9's e2e test re-runs `pyspark_driver.py` through a fake spark-submit
  shim that re-execs the driver as a Python process. That's the whole point —
  it lets us prove `Resolver → generate → shell → driver` end-to-end without
  an actual Spark install.
- `openspec validate` shows the change as 7/7 artefacts complete. That refers
  to the *design artefacts* being filled in, NOT to implementation. Real
  progress lives in `tasks.md` checkboxes.

## When the user says "continue"

Default route: continue the implementation path defined in
`tasks.md` group order, currently **5.x Snapshot Service → 7.x Publish →
8.x DS Adapter → 10.x Instance Service → 13.x Frontend**. Don't skip ahead;
each group's contract is consumed by the next.

If the user says "stop" or "pause", do not silently proceed past the
current group. Ask before crossing a group boundary.
