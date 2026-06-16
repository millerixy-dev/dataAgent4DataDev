#!/usr/bin/env bash
#
# Idempotent metastore schema bootstrap.
#
# Note: this script REPLACES the apache/hive image's stock entrypoint, so it
# also has to do the work that entrypoint does: symlink anything under
# ${HIVE_CUSTOM_CONF_DIR} into ${HIVE_HOME}/conf so schematool sees our
# hive-site.xml. Without that, schematool falls back to the bundled
# derby-flavoured hive-default.xml and ignores the postgres connection URL.
#
# Idempotency strategy: probe the metastore with `schematool -info`. It
# exits 0 when a schema is already present and prints the schema version;
# exits non-zero (with "Failed to get schema version" / "MetaException")
# when the metastore DB is empty. We treat exit 0 as "skip initSchema".
# `schematool -info` is read-only, so probing is safe.
set -euo pipefail

if [ -z "${HIVE_HOME:-}" ]; then
  HIVE_HOME=/opt/hive
fi
export HIVE_CONF_DIR="${HIVE_HOME}/conf"

# Mirror the stock entrypoint behaviour: symlink custom conf files into
# HIVE_CONF_DIR so schematool actually sees hive-site.xml.
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}/" \;
  export HADOOP_CONF_DIR="${HIVE_CONF_DIR}"
fi

DB_TYPE="${DB_DRIVER:-postgres}"

echo "[hive-init] probing existing schema with schematool -info"
if "${HIVE_HOME}/bin/schematool" -dbType "${DB_TYPE}" -info >/tmp/schematool-info.log 2>&1; then
  version="$(grep -E 'Metastore schema version' /tmp/schematool-info.log | awk -F: '{print $2}' | tr -d ' \t')"
  echo "[hive-init] schema already present (version=${version:-unknown}), skipping initSchema"
  exit 0
fi
echo "[hive-init] schematool -info exited non-zero — assuming empty DB"

echo "[hive-init] running schematool -initSchema -dbType ${DB_TYPE}"
"${HIVE_HOME}/bin/schematool" -initSchema -dbType "${DB_TYPE}"
echo "[hive-init] initSchema OK"
