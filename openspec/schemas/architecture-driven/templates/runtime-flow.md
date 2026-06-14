## Runtime Scope

- **Actors/triggers:**
- **Entry points:**
- **Preconditions:**
- **Runtime invariants:**
- **Excluded flows:**

## Primary Interaction Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Caller
    participant Entry as Entry / Adapter
    participant App as Application / Orchestrator
    participant Core as Domain / Core Logic
    participant Dep as External Dependency
    participant Store as Data Store

    Caller->>Entry: Request / trigger
    Entry->>App: Validated command
    App->>Core: Execute use case
    Core->>Dep: Dependency call
    Dep-->>Core: Result
    Core->>Store: Persist state/result
    Store-->>Core: Commit acknowledgement
    Core-->>App: Outcome
    App-->>Entry: Response model
    Entry-->>Caller: Observable result
```

### Sequence Notes

| Step | Preconditions | Action | Timeout | Retry/idempotency | Observable result |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Control Flow and Branches

```mermaid
flowchart TD
    start([Start])
    auth{Authorized?}
    valid{Input valid?}
    execute[Execute operation]
    dependency{Dependency succeeded?}
    retry{Retry budget remains?}
    compensate[Compensate / rollback]
    success([Success])
    reject([Reject])
    fail([Terminal failure])

    start --> auth
    auth -->|No| reject
    auth -->|Yes| valid
    valid -->|No| reject
    valid -->|Yes| execute
    execute --> dependency
    dependency -->|Yes| success
    dependency -->|No| retry
    retry -->|Yes| execute
    retry -->|No| compensate
    compensate --> fail
```

### Branch Conditions

| Branch/decision | Condition | Source of truth | Result/action | Spec scenario |
|---|---|---|---|---|
|  |  |  |  |  |

## Lifecycle State Machine

<!-- Keep when an entity/job/request has meaningful lifecycle states; otherwise state why it is not applicable. -->

```mermaid
stateDiagram-v2
    [*] --> Pending
    Pending --> Running: accepted / scheduled
    Running --> Succeeded: completed
    Running --> Retrying: retryable failure
    Retrying --> Running: retry
    Running --> Failed: terminal failure
    Pending --> Cancelled: cancel
    Running --> Cancelling: cancel requested
    Cancelling --> Cancelled: cleanup complete
    Succeeded --> [*]
    Failed --> [*]
    Cancelled --> [*]
```

| State | Entry condition | Allowed transitions | Persistent fields | Timeout | Terminal? |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Failure and Recovery Paths

| Failure point | Detection | Retry policy | Fallback/compensation | User/operator signal | Terminal condition |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Concurrency and Idempotency

- **Concurrency model:**
- **Locking/serialization:**
- **Idempotency key and scope:**
- **Duplicate request behavior:**
- **Race conditions and prevention:**
- **Backpressure/rate limiting:**

## Timeouts, Retries, and Circuit Breaking

| Call/operation | Timeout | Attempts | Backoff/jitter | Retryable errors | Circuit/fallback |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Cancellation, Compensation, and Manual Intervention

- **Cancellation points:**
- **Cleanup behavior:**
- **Compensation order:**
- **Irreversible side effects:**
- **Manual runbook/intervention:**

## Observability of Runtime Flow

| Signal | Name/fields | Emission point | Correlation key | Alert/SLO |
|---|---|---|---|---|
| Log |  |  |  |  |
| Metric |  |  |  |  |
| Trace |  |  |  |  |
| Audit event |  |  |  |  |

## Scenario-to-Flow Mapping

| Spec scenario | Sequence steps | Branch/state path | Test level | Evidence |
|---|---|---|---|---|
|  |  |  |  |  |
