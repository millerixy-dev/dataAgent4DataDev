# dataAgent4DataDev

A data-development platform on top of CDP 7.1 + DolphinScheduler 3.1.7 + Spark 2.4.7.

The frontend exposes a project tree, SQL editor, Spark config form, and scheduler;
the backend turns each "publish" into an immutable HDFS snapshot, generates the
spark-submit command server-side (whitelist + shell-quote escaping), and uses DS
purely as a timer + dependency graph. A thin `pyspark_driver.py` runs inside the
YARN cluster Driver container.

Status: **MVP groups 1–4 implemented and end-to-end tested locally**. Snapshot
Service, Publish Orchestrator, DS Adapter, Instance Service, Frontend remain to
be built; see `openspec/changes/data-dev-platform/tasks.md` for the live progress
checklist.

## Quick start

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
# install dev deps
uv sync --group dev

# run the full test suite (driver + shell + resolver + command-generator + e2e)
uv run pytest

# lint
uv run ruff check .
shellcheck --severity=warning spark_submit.sh   # optional
```

`pyspark` itself is **not** a dev dependency. The driver imports it only inside
its `main()` SparkSession path, so all 100 tests run in any environment.

## Repository layout

```
.
├── contracts/runtime_variables.yaml   Cross-stack contract: driver / Resolver
│                                       / frontend variable panel all read this.
│                                       Declares ${dt}, ${date}, ${month},
│                                       ${dt-N}, ${date-N}, ${hr}.
├── pyspark_driver.py                   YARN cluster-mode entry script.
│                                       Imports pyspark only inside main().
├── pyspark_driver_pkg/                 The pyspark-free orchestration layer.
│   ├── variable_catalog.py              Loads contracts/*.yaml.
│   ├── renderer.py                      Renders ${dt}/${dt-N}/${hr}/...
│   ├── sql_splitter.py                  Splits SQL on ; respecting quotes.
│   ├── driver.py                        argparse + run_render() (no pyspark).
│   ├── resolver.py                      Publish-time bake + preview.
│   └── command_generator.py             Backend spark-submit command builder.
├── spark_submit.sh                     DS Worker entry: kinit + eval $SPARK_CMD
│                                       + appId capture + platform callback.
├── t_eci_company_4_dwi.sql             Production SQL kept as a regression
│                                       fixture for driver upgrades.
├── tests/                              pytest suite (100 cases). Includes two
│                                       end-to-end runs that stitch
│                                       Resolver → snapshot file →
│                                       spark_submit.sh → driver --dry-run.
├── pyproject.toml                      uv-managed Python project.
└── openspec/                           OpenSpec design assets (see below).
```

## Architecture

The full picture lives in OpenSpec — see `openspec/changes/data-dev-platform/`:

- **proposal.md** — why the change is needed, what capabilities it introduces.
- **specs/** — twelve capability specs, each in `WHEN/THEN` scenario form.
- **architecture.md** — system context, component diagrams, deployment topology.
- **data-flow.md** — every data object's lifecycle, contracts, retention.
- **runtime-flow.md** — sequence diagrams + state machines + failure paths.
- **design.md** — eleven resolved decisions and the MVP scope cut.
- **tasks.md** — implementation checklist tracking progress.

Three architectural invariants you should know before touching code:

1. **DS does not own user material.** SQL and the driver script live in HDFS
   snapshots; DS is just a timer + dependency graph + worker pool.
2. **Snapshots are immutable.** Rollback flips a pointer; nothing on HDFS is
   ever overwritten or deleted. `version_id` is monotonically allocated and
   never reused.
3. **The backend is the only writer of spark-submit commands.** DS shell body
   is `eval "$SPARK_CMD"` — no parameter assembly happens on the worker.

Variable resolution is split into two layers:

- Project variables (`${prj.warehouse}`) bake at publish time into the snapshot.
- Runtime variables (`${dt}`, `${dt-N}`, `${hr}`) render at the driver via
  `--biz-date` / `--biz-hour`.

## Working on this repo

Read **CLAUDE.md** before making changes. It captures the conventions
(uv-only, OpenSpec-first, TDD, no pyspark in tests) you'll need either as a
human contributor or via an AI assistant.

When you change behaviour:

1. Find the relevant capability spec in `openspec/changes/data-dev-platform/specs/`
   and update its `WHEN/THEN` scenarios.
2. Write a failing test in `tests/`.
3. Implement.
4. Tick the matching line in `tasks.md`.
5. Commit using Conventional Commits (`feat(driver): ...`, `refactor(shell): ...`).

`openspec validate data-dev-platform` must stay green.

## License

TBD.
