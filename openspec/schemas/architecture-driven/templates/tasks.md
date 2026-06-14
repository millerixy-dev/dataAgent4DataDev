## 1. Design Readiness

- [ ] 1.1 Resolve blocking open questions and record final decisions in `design.md`
- [ ] 1.2 Review requirement-to-component, data-flow, and runtime-flow mappings
- [ ] 1.3 Confirm migration, rollback, security, and operational acceptance gates

## 2. Interfaces and Foundations

- [ ] 2.1 Implement or update public/internal contracts and compatibility handling
- [ ] 2.2 Implement required configuration, dependency wiring, and feature flags
- [ ] 2.3 Add contract/schema validation and foundational unit tests

## 3. Core Implementation

- [ ] 3.1 Implement the first dependency-ordered functional slice
- [ ] 3.2 Implement remaining domain/application behavior
- [ ] 3.3 Implement persistence, messaging, external integrations, and failure handling

## 4. Data and Migration

- [ ] 4.1 Implement schema/storage changes and migration/backfill logic
- [ ] 4.2 Add data-quality, idempotency, replay, and reconciliation controls
- [ ] 4.3 Test forward migration, backward compatibility, and rollback behavior

## 5. Runtime Reliability and Security

- [ ] 5.1 Implement authorization, validation, secrets, audit, and abuse controls
- [ ] 5.2 Implement timeout, retry, circuit-breaker, cancellation, and compensation behavior
- [ ] 5.3 Verify concurrency, race-condition, duplicate-request, and backpressure behavior

## 6. Verification

- [ ] 6.1 Add unit tests mapped to requirements and design decisions
- [ ] 6.2 Add integration/contract tests for component and data boundaries
- [ ] 6.3 Add end-to-end tests for success, failure, boundary, and permission scenarios
- [ ] 6.4 Run performance/resilience/security checks required by the design

## 7. Observability and Operations

- [ ] 7.1 Add structured logs, metrics, traces, audit events, and correlation identifiers
- [ ] 7.2 Create or update dashboards, alerts, runbooks, and support ownership
- [ ] 7.3 Validate deployment ordering, health checks, readiness, and rollback procedure

## 8. Documentation and Design Conformance

- [ ] 8.1 Update user/developer/API/operations documentation
- [ ] 8.2 Compare the as-built implementation with all OpenSpec artifacts
- [ ] 8.3 Update `architecture.md`, `data-flow.md`, `runtime-flow.md`, and `design.md` for approved implementation drift
- [ ] 8.4 Run OpenSpec validation and record final verification evidence
