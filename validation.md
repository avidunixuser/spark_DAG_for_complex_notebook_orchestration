# Recorded reference validation

The machine-readable reports include timestamps, runtime versions, exact
configuration/code fingerprints, individual test outcomes, and the nine
failure-injection subtests.

| Execution environment | Result | Scope |
|---|---|---|
| Linux/aarch64, Python 3.12, Spark 4.0.1, Delta 4.0.0 | **155 passed, 4 browser checks explicitly skipped** | Canonical XML parsing, distributed ingestion, real Spark recovery, native Delta transactions, media, and portable runtime checks |
| Windows, Python 3.13, Microsoft Edge | **149 passed, 10 Spark/Delta checks explicitly skipped** | Canonical XML, real child-process termination/recovery, mocked cloud APIs, generated media, and all four browser playback checks |

Evidence: [test-results.json](test-results.json) and
[test-results.windows.json](test-results.windows.json).
Each report contains 159 discovered tests and no failures or errors.

## Failure-injection matrix

The named subtests in
`ExecutionTests.test_failure_injection_into_every_restartable_node` passed for:

| Injected node | Observed assertions |
|---|---|
| `extract_shipments` | XML ingestion failed, remediated, reran, downstream receiving plan correct |
| `extract_products` | XML catalog ingestion failed, remediated, reran, downstream receiving plan correct |
| `validate_shipments` | Valid extracts reused, failed node repaired, descendants completed |
| `quality_gate` | Valid upstream work reused, gate repaired, receiving plan completed |
| `plan_receipts` | Valid upstream work reused, warehouse/SKU/unit quantities recomputed correctly |
| `request_receipts` | Repaired inventory-receipt intent, exactly one durable intent |
| `notify_failure` | Repaired while its activating failure remained; full workload recovered afterward |
| `notify_timeout` | Repaired while its activating timeout remained; full workload recovered afterward |
| `cleanup` | Valid main-path work reused; completion bookkeeping repaired |

Every recovered plan had **120 `SKU-FILTER` units for `WH-ATL` across two accepted
shipment lines** and **60 `SKU-SEAL` units for `WH-DFW` across one line**, all in
`EA`, with exactly one inventory-receipt intent in that scenario's
isolated control store. Separate tests cover a crash after outbox commit,
force-rerun deduplication, changed-payload conflicts, and repair of a pending
intent's missing output reference.

## Requirements coverage

| Requested scenarios | Evidence in `tests/` |
|---|---|
| 1-6: configuration and notebook definitions | `test_configuration.py`, `test_dag.py` |
| 7-8: cycles and missing dependencies | `test_dag.py` |
| 9-12: happy path, parallelism, fan-in, conditions | `test_execution.py` |
| 13-18: failure matrix, recovery, timeout, retries, skips | `test_execution.py`, `test_adapters.py` |
| 19-23: force modes, repeated failures, config changes, missing outputs | `test_execution.py` |
| 24-27: collisions, side effects, blocking, complete audit | `test_control_store.py`, `test_execution.py`, `test_adapters.py` |
| 28: data correctness across restart | `test_execution.py`, `test_spark.py` |
| Canonical XML intake | Multi-file/recursive ingestion, namespace/version/shape checks, DTD/entity rejection, resource limits, compound keys, SKU/unit validation, malformed-file recovery |
| Additional integrity/performance checks | Native Delta commit/version/identity tests, incremental history across writers, attempt-scoped proof reuse, source metadata fingerprints, Spark cache cleanup, clock-skew regression, capped retries, eager resource bounds, and linear graph traversal |

The wheel build, configuration/DAG validation, notebook format/code validation,
Ruff lint, and formatting checks also completed successfully.

The scheduling benchmark and its exact scope are documented in
[performance.md](performance.md), with measurements in
[performance-results.json](performance-results.json).

## Not established by these results

- No original DataStage workload was supplied; equivalence/conversion acceptance
  is **not** established.
- Fabric, Databricks Jobs, and ADLS Gen2 file APIs were exercised through contract
  fakes, not authenticated live services. Native Delta tests use a test-only
  in-process lease implementation.
- No real WMS posting, external receiver exactly-once guarantee, cloud identity,
  networking, production capacity/SLA, or cloud failure/cancellation behavior has
  been certified.
- The native Windows Spark attempt encountered a host/runtime Python worker
  error; the passing real Spark/Delta checks ran in Linux with its native
  filesystem and supported Java, not through an unsupported SQLite network mount.
