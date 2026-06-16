# Local development stack

Lightweight (A-line) docker stack for working on groups 5–8 of the data-dev-platform
without paying for CDP licences. Brings up:

| Service                | Image / version                                | Purpose                                |
|------------------------|------------------------------------------------|----------------------------------------|
| Postgres               | `postgres:14`                                  | Hive Metastore metadata DB             |
| Hive init (one-shot)   | `apache/hive:3.1.3`                            | `schematool -initSchema` then exits    |
| Hive Metastore         | `apache/hive:3.1.3`                            | Thrift `:9083`, web UI `:10002`        |
| DolphinScheduler       | `apache/dolphinscheduler-standalone-server:3.1.7` | Master+Worker+API+UI in one container |

What's intentionally **not** here (deferred to B-line / staging):

- HDFS NameNode / DataNode — warehouse lives on `compose/warehouse/`, a host bind mount.
- YARN ResourceManager / NodeManager — driver runs `local[*]` in tests.
- Kerberos KDC — A-line is plain auth.

If you need spark-submit-on-YARN or SPNEGO Kerberos, switch to the B-line stack
(documented in `openspec/changes/data-dev-platform/design.md` MVP Scope).

## Quick start

```bash
# from repo root
make up           # starts postgres + hive-init + hive-metastore, waits for healthy
make ps           # see what's running
make logs         # tail logs (Ctrl-C to leave)
make down         # stop (keeps volumes)
make clean        # stop + drop volumes
```

`make up` blocks until healthchecks pass. First boot is ~60 s (schematool runs once);
subsequent boots are ~10 s because the init container short-circuits.

DolphinScheduler is opt-in:

```bash
make up-ds        # adds DS standalone container
# UI:    http://localhost:12345/dolphinscheduler/ui   (admin / dolphinscheduler123)
# API:   http://localhost:12345/dolphinscheduler
make down-ds
```

## Wiring code to the stack

Group 5 `LocalFsBackend` already points at any directory you give it. Pointing
it at `compose/warehouse/` from your dev machine works because the container
sees the same files via `/opt/hive/warehouse`.

A pyspark client running on your host can connect to the metastore with::

    spark = (
        SparkSession.builder
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "<repo-root>/compose/warehouse")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate()
    )

End-to-end tests that depend on the stack are tagged ``@pytest.mark.compose``;
use ``make test-compose`` (or ``uv run pytest -m compose``) to run them.

## Ports

Defaults can be overridden in `compose/.env` (copy from `compose/.env.example`).

| Service          | Default | Purpose                                   |
|------------------|---------|-------------------------------------------|
| Postgres         | 15433   | mapped to container `:5432`               |
| Hive Metastore   | 19083   | thrift                                    |
| Hive Metastore   | 10012   | web UI                                    |
| DolphinScheduler | 12345   | UI + Open API                             |
| DolphinScheduler | 25333   | python gateway                            |

Defaults are intentionally non-standard (no plain `:9083`, no `:5432`) so this
stack can coexist with another CDP-flavoured docker stack on the same machine.

## Volumes & data layout

```
compose/
  warehouse/      Hive table data — mirrored at /opt/hive/warehouse in HMS
  hive-conf/      hive-site.xml mounted read-only
  hive-init/      run.sh (idempotent schematool wrapper)
  .env            (gitignored) tweaks defaults
```

Postgres data lives in the named volume `dataagent-pg-data`. `make clean` drops
it; `make wipe-warehouse` clears Hive table files without touching the
metastore (handy for "clean tables, keep schema").

## Common chores

```bash
make hms-shell        # psql into metastore DB
docker exec -it dataagent-hive-metastore beeline   # interactive HMS via beeline
```

## Troubleshooting

- **`hive-init` keeps re-running schematool and erroring** — usually means an
  older schema exists with a version mismatch. `make clean` then `make up`.
- **Port 5432/9083/12345 already in use** — copy `.env.example` to `.env` and
  bump the port.
- **macOS file-share slowness** — bind mounts on Docker Desktop are slow on
  large data. The metastore itself uses a named volume; only `warehouse/` is
  bind-mounted, so this affects table reads/writes but not metastore ops.
