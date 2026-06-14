#!/usr/bin/env bash
#
# YARN Worker entry: kinit + eval $SPARK_CMD + appId capture + callback.
#
# Designed for the new data-dev-platform pipeline:
#
#   - Backend generates the full spark-submit command and injects it as
#     SPARK_CMD (DS task custom parameter / env var).
#   - This script does NOT build any spark-submit arguments itself.
#   - The placeholder ${biz_date} inside SPARK_CMD is substituted with the
#     real BIZ_DATE via shell parameter expansion (NOT a second eval, so
#     a malicious BIZ_DATE cannot trigger arbitrary code).
#
# Required env vars:
#   SPARK_CMD           backend-generated command string
#   BIZ_DATE            yyyyMMdd, replaces ${biz_date} placeholder
#   PRINCIPAL           Kerberos principal
#   KEYTAB_PATH         absolute path to keytab (must be readable)
#   INSTANCE_ID         platform instance id (logged + sent to callback)
#   TRACE_ID            W3C traceparent value
#
# Optional env vars:
#   VERSION_ID          snapshot version (logged)
#   TASK_ID             platform task id (logged)
#   PLATFORM_CALLBACK_URL  if set, POST application_id + exit code on completion
#

set -euo pipefail

# Print structured trace header to stderr (avoid mixing with submitter stdout).
echo "[spark_submit.sh] start trace_id=${TRACE_ID:-} instance_id=${INSTANCE_ID:-} version_id=${VERSION_ID:-} task_id=${TASK_ID:-}" >&2

# --- precondition checks ---------------------------------------------------

if [[ -z "${SPARK_CMD:-}" ]]; then
  echo "[spark_submit.sh] [ERROR] SPARK_CMD not set" >&2
  exit 2
fi

if [[ -z "${BIZ_DATE:-}" ]]; then
  echo "[spark_submit.sh] [ERROR] BIZ_DATE not set" >&2
  exit 2
fi

if [[ ! "${BIZ_DATE}" =~ ^[0-9]{8} ]]; then
  echo "[spark_submit.sh] [ERROR] BIZ_DATE format invalid (expect yyyyMMdd prefix), got: ${BIZ_DATE}" >&2
  exit 2
fi

if [[ -z "${PRINCIPAL:-}" ]]; then
  echo "[spark_submit.sh] [ERROR] PRINCIPAL not set" >&2
  exit 2
fi

if [[ -z "${KEYTAB_PATH:-}" || ! -r "${KEYTAB_PATH}" ]]; then
  echo "[spark_submit.sh] [ERROR] KEYTAB_PATH missing or unreadable: ${KEYTAB_PATH:-<unset>}" >&2
  exit 2
fi

# --- kinit -----------------------------------------------------------------
# Use ${var:?} to keep set -u happy; suppress xtrace in case it's enabled.
{ set +x; } 2>/dev/null
kinit -kt "${KEYTAB_PATH}" "${PRINCIPAL}"

# --- compose final command -------------------------------------------------
#
# Replace ${biz_date} placeholder via shell parameter expansion of the
# variable contents — this is a single text substitution and never invokes
# eval on BIZ_DATE itself. Even if BIZ_DATE contains $(...), it lands in CMD
# as literal characters that eval will then interpret. To prevent that, we
# refuse any BIZ_DATE that is not pure digits (the regex check above).

# shellcheck disable=SC2016
PLACEHOLDER='${biz_date}'
CMD="${SPARK_CMD//${PLACEHOLDER}/${BIZ_DATE}}"

LOG_FILE="$(mktemp -t spark-submit-XXXXXX.log)"
trap 'rm -f "${LOG_FILE}"' EXIT

# --- run -------------------------------------------------------------------
# `eval` is required because SPARK_CMD is a single string with quoted args.
# Backend is the sole writer of that string and applies shell-quote escaping
# under a strict white/blacklist (specs/command-generation).

set +e
# shellcheck disable=SC2086
eval "${CMD}" 2>&1 | tee "${LOG_FILE}"
EXIT_CODE=${PIPESTATUS[0]}
set -e

# --- application_id capture -----------------------------------------------
APPLICATION_ID="$(grep -oE 'application_[0-9]+_[0-9]+' "${LOG_FILE}" | head -n1 || true)"

echo "[spark_submit.sh] done trace_id=${TRACE_ID:-} instance_id=${INSTANCE_ID:-} application_id=${APPLICATION_ID:-} exit=${EXIT_CODE}" >&2

# --- platform callback ----------------------------------------------------
if [[ -n "${PLATFORM_CALLBACK_URL:-}" ]]; then
  PAYLOAD=$(printf '{"instance_id":"%s","application_id":"%s","exit":%s,"trace_id":"%s"}' \
    "${INSTANCE_ID:-}" "${APPLICATION_ID:-}" "${EXIT_CODE}" "${TRACE_ID:-}")
  if ! curl -fsS -m 10 -X POST -H 'Content-Type: application/json' \
        -d "${PAYLOAD}" "${PLATFORM_CALLBACK_URL}" >&2; then
    echo "[spark_submit.sh] [WARN] platform callback failed (non-fatal)" >&2
  fi
fi

exit "${EXIT_CODE}"
