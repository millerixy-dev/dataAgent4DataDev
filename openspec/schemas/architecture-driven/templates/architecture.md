## Scope and Inputs

<!-- Summarize the change boundary and list proposal/specs/code/config/deployment sources examined. -->

- **Change boundary:**
- **Existing constraints:**
- **Assumptions:**
- **Out of scope:**

## Current-State Summary

<!-- Explain only the existing structure needed to understand this change. -->

## System Context

```mermaid
flowchart LR
    actor[Actor / Caller]
    system[System Under Change]
    external[External System]

    actor -->|Request / command| system
    system -->|API / event / file| external
```

### Context Participants

| Participant | Type | Responsibility | Owner | Trust boundary |
|---|---|---|---|---|
|  |  |  |  |  |

## Target Component Architecture

```mermaid
flowchart TB
    subgraph boundary[System Boundary]
        entry[Entry / Adapter]
        app[Application / Orchestrator]
        domain[Domain / Core Logic]
        store[(Data Store)]

        entry --> app
        app --> domain
        domain --> store
    end

    dependency[External Dependency]
    app --> dependency
```

### Component Responsibilities

| Component | Responsibility | Inputs | Outputs | State owned | Failure boundary |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Interfaces and Dependencies

| From | To | Interface/protocol | Sync/async | Contract/version | Timeout/SLA | Compatibility |
|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |

## Deployment Topology

<!-- Keep this section when deployment/process/network placement matters; otherwise state “Not applicable” and why. -->

```mermaid
flowchart TB
    subgraph zone1[Trust Zone / Cluster]
        service[Service / Process]
        database[(Database / Storage)]
        service --> database
    end

    client[Client / Upstream]
    client -->|Network protocol| service
```

| Runtime unit | Environment/node | Scaling model | Configuration/secrets | Health/readiness | Owner |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

## Security and Trust Boundaries

- **Authentication:**
- **Authorization:**
- **Secrets:**
- **Network boundaries:**
- **Sensitive operations/data:**
- **Audit requirements:**

## Capability-to-Component Mapping

| Capability / requirement | Entry component | Owning component | Supporting components | Notes |
|---|---|---|---|---|
|  |  |  |  |  |

## Architectural Constraints and Invariants

- 

## Alternatives and Open Decisions

| Question/decision | Options | Current preference | Evidence needed | Owner/status |
|---|---|---|---|---|
|  |  |  |  |  |
