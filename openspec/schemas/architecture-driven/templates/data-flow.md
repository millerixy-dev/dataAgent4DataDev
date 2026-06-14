## Scope and Data Inventory

| Data object | Producer | Consumers | Classification | Source of truth | Retention |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## End-to-End Data Flow

```mermaid
flowchart LR
    source[Producer / Source]
    ingest[Ingestion / Validation]
    process[Transform / Business Processing]
    store[(Persistent Store)]
    consumer[Consumer / Sink]
    reject[(Reject / DLQ / Quarantine)]

    source -->|Input contract| ingest
    ingest -->|Valid records| process
    ingest -->|Invalid records| reject
    process -->|Write / upsert| store
    store -->|Read / publish| consumer
```

### Flow Steps

| Step | From → To | Data/contract | Transformation | Validation | Persistence | Failure destination |
|---|---|---|---|---|---|---|
| 1 |  |  |  |  |  |  |

## Data Contracts

### <data object or interface>

| Field | Type | Required | Meaning | Constraints | Sensitive | Compatibility rule |
|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |

- **Contract/version identifier:**
- **Primary/business key:**
- **Idempotency/deduplication key:**
- **Partitioning/bucketing:**
- **Ordering guarantees:**
- **Null/default semantics:**
- **Schema evolution policy:**

## Transformations and Business Rules

| Transformation | Input | Output | Rule/algorithm | Deterministic | Error handling |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Storage and Lifecycle

| Store | Purpose | Write pattern | Read pattern | Consistency | Retention/deletion | Backup/recovery |
|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |

## Data Quality Controls

| Control | Stage | Rule/threshold | Blocking? | Metric/evidence | Remediation |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Failure, Replay, and Recovery Flow

```mermaid
flowchart TD
    receive[Receive data]
    validate{Valid?}
    process[Process]
    success[Commit / Publish success]
    retry{Retryable?}
    retryq[(Retry queue / checkpoint)]
    quarantine[(Quarantine / DLQ)]

    receive --> validate
    validate -->|Yes| process
    validate -->|No| quarantine
    process -->|Success| success
    process -->|Failure| retry
    retry -->|Yes| retryq
    retryq --> process
    retry -->|No| quarantine
```

- **Replay boundary:**
- **Checkpoint/offset semantics:**
- **Duplicate handling:**
- **Partial-write handling:**
- **Recovery objective:**

## Security, Privacy, and Governance

- **Classification and sensitive fields:**
- **Encryption in transit/at rest:**
- **Masking/tokenization:**
- **Access control:**
- **Retention/deletion/legal hold:**
- **Audit and lineage:**

## Capacity and Freshness

| Measure | Current | Expected | Peak | Limit/SLO | Scaling response |
|---|---|---|---|---|---|
| Volume |  |  |  |  |  |
| Throughput |  |  |  |  |  |
| Payload size |  |  |  |  |  |
| Freshness/latency |  |  |  |  |  |

## Requirement-to-Data-Flow Mapping

| Requirement/scenario | Flow steps | Data objects | Quality/security controls | Evidence |
|---|---|---|---|---|
|  |  |  |  |  |
