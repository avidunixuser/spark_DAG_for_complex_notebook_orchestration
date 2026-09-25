# Operator runbook

## Read the result first

The CLI exits nonzero and the orchestrator notebook raises an actionable error
when critical nodes fail/block/time out/cancel, or execution is uncertain. The
durable summary records all node states, restart mode/origin/reasons, failures,
and a recommended action. `SUCCEEDED` may still contain noncritical failures or
inactive branches; review the details rather than relying only on a pipeline
green indicator.

Local inspection:

```powershell
.\.venv\Scripts\python.exe -m spark_dag inspect --business-run-key 2026-09-24 --run-id "<run-id>"
```

Cloud inspection from a trusted notebook with the deployed package:

```python
from spark_dag.config_loader import load_config
from spark_dag.model import fingerprint
from spark_dag.runtime import build_services

config = load_config(config_file, deployment_dir=deployment_dir, environment=environment)
services = build_services(config, spark=spark, notebookutils=notebookutils_if_fabric)
scope = fingerprint([config.data["application_id"], config.data["dag"]["id"], business_run_key])
run = services.store.run(scope, run_id)
nodes = services.store.nodes(scope, run_id)
```

Inspect only metadata in shared logs. Do not print credentials, parameters,
source rows, reject payloads, raw platform exceptions, or entire control payloads
to a broadly accessible console.

## Choose the correct recovery mode

| Intent | Mode and requirements |
|---|---|
| First attempt for a new business key | `normal`; no parent or approvals |
| Recover latest failed run | `resume`, latest failed `parent_run_id`, non-sensitive incident ID |
| Recompute one node/checkpoint and descendants | `force_restart`, latest parent, `restart_from`, change/incident ID |
| Repeat every eligible node for the business key | `force_rerun`, change ID; successful previous work is not reused |
| Retry transient failure inside a run | Automatic only within that node's configured safe retry policy |
| Manually retry an exhausted failed node | Remediate, then resume or force restart from that node; do not directly invoke a child with a fabricated token |
| Repeat a non-idempotent operation | Required recovery mode plus explicit `--approve-node`; compensation-class nodes also need a verified companion checkpoint |

Force rerun is **not** a bypass for leases, manual approval, compensation, output
validation, branch conditions, or receiver duplicate protection. Setting both
force flags, mixing incompatible config/runtime modes, or omitting the restart
origin fails immediately.

Use a non-sensitive ticket such as `INC-123` or `CHG-456` for `reason`. Do not put
personal data, credentials, or incident narratives into that field.

## Ordinary failed run

1. Confirm the run is `FAILED`, not `RUNNING` or `RECOVERY_REQUIRED`, and every
   relevant worker is terminal.
2. Diagnose by node ID/error code/category and approved platform diagnostics.
   Fix the source snapshot, capacity, quality rule, or component as appropriate.
3. For code/config changes, publish an immutable release. Do not modify files
   beneath running notebook attempts.
4. Resume using the latest failed run ID. Observe
   `SKIPPED_ALREADY_SATISFIED` only after proof validation; missing/stale outputs
   and affected descendants must execute again.
5. Verify the final report, rejects, outbox, and critical node states. A second
   failed recovery becomes the parent for the next recovery.

Do not edit a control event to make a failed node appear successful.

## Uncertain or abandoned work: mandatory quarantine procedure

`RECOVERY_REQUIRED`, an abandoned `RUNNING` record, or a retained transaction
lease is a **stop condition**, not permission to rerun.

1. Disable the outer schedule and prevent new writers. Identify the exact
   application/DAG/business key, control-table path, release, run ID, scope hash,
   remote task IDs, and incident ID.
2. Confirm **all** orchestrator and child execution has stopped, including
   retries, Spark jobs, driver processes, external commands, asynchronous
   writes, and delivery consumers that could still mutate the same output.
   On Databricks, request cancellation and observe a terminal Jobs API state.
   On Fabric, reconcile/cancel the affected notebook/session using supported
   platform controls. Do not stop unrelated shared sessions.
3. If submission acknowledgement was lost, reconcile against platform run
   history and the recorded attempt context. Do not blindly submit a new task
   merely to discover whether a previous task ran.
4. Inspect committed output versions, orphaned artifacts, and outbox intents.
   An uncertain side effect may have happened. Check the receiver's idempotency
   records or require business-owner approval/compensation.
5. Only after quiescence is proven, use the Azure Storage operator tooling to
   break/release the **specific abandoned ADLS file lease(s)**. Never break every
   lease in the file system. Do not delete lock files or alter lease duration.
6. Reconcile the run with `workers_stopped=True` and the incident ID. This
   records cancelled/unacknowledged attempts and changes the run to `FAILED`.
   The method does not stop workers or break Azure leases for you.
7. Resume from that latest failed run and verify data and effects before
   re-enabling the schedule.

Lock file paths, relative to `locking.file_system`, are:

```text
<locking.directory>/<sha256(canonical_json(lock_name))>.lock
run lock_name:        run:<control_store.path>:<scope>
transaction name:    control:<control_store.path>:<scope>
initialization name: initialize:<control_store.path>
```

`fingerprint()` in `spark_dag.model` computes this exact digest. The run and
control locks are different; an ambiguous Delta commit can retain the control
mutex as well. Do not change the DFS account/file-system/directory to evade a lock.
Lease IDs are not required to identify the hashed lock file and should not be printed
in public logs.

For an approved, specifically identified orphan, `DataLakeLeaseClient` supports
`break_lease(lease_break_period=0)` on the corresponding `DataLakeFileClient`.
This is an operator action only, after the quiescence checks above; the runtime
never invokes it. OneLake coordination files, where validated, must live under
the Lakehouse's `Files` area, never at a managed workspace/item root.

Local reconciliation, **only after personally confirming the processes stopped**:

```powershell
.\.venv\Scripts\python.exe -m spark_dag reconcile --business-run-key 2026-09-24 --run-id "<abandoned-run-id>" --workers-stopped --reason INC-123
```

Cloud reconciliation, after the specific abandoned leases are safely resolved:

```python
services.store.reconcile(scope, run_id, workers_stopped=True, reason="INC-123")
```

The confirmation flag is an operator attestation, not an automatic process-liveness
detector. Never delete SQLite/WAL files or the Delta control table to bypass it.

## Common diagnoses

| Error / condition | Action |
|---|---|
| `MISSING_SIDECAR`, `SIDECAR_LOCATION` | Correct `deployment_dir`/`config_file`; check driver visibility and permissions |
| `INVALID_CONFIGURATION`, `UNSUPPORTED_SCHEMA` | Validate against the deployed schema/version; do not silently fall back to defaults |
| `UNCONFIGURED_PLATFORM` | Replace required placeholders in the selected environment |
| `CONFIGURATION_CHANGED`, `INPUT_CHANGED` | Stop mutable-deployment/input practices; use a new immutable release/snapshot |
| `CLAIM_COLLISION`, lease collision | Check an active/abandoned owner; never launch a competing orchestrator |
| `SOURCE_UNAVAILABLE` | Restore/land the declared snapshot; preserve its version and schema |
| `QUALITY_GATE_FAILED`, `INVALID_DIMENSION` | Inspect restricted reject data/key rules; obtain approval for any rule change |
| `OUTPUT_INVALID`, `UPSTREAM_OUTPUT_INVALID` | Restore or recompute invalid data; dependent successes must not be reused |
| `APPROVAL_REQUIRED`, `COMPENSATION_REQUIRED` | Obtain explicit authorization and validate real business compensation |
| `SIDE_EFFECT_CONFLICT` | A delivery identity already represents different content. Do not overwrite it; use an approved correction/revision process |
| `REMOTE_STATE_UNKNOWN`, `CANCELLATION_UNCONFIRMED`, `FABRIC_STATE_UNKNOWN` | Follow quarantine, not an automatic retry |
| `LEASE_LOST`, `LEASE_RELEASE_FAILED` | Restore lock-service access and reconcile before another writer starts |
| `UNEXPECTED_ERROR` | Treat as non-retryable by default; use access-controlled diagnostics and fix the component/runtime |

Native platform errors before a child can persist failure may be conservatively
classified as infrastructure uncertainty. Availability is intentionally sacrificed
rather than risk concurrent duplicate writes.

## Failure-injection exercise

Use synthetic data and a separate test business key/control/output namespace.
Copy the sidecar to a different filename **in the same deployment folder**,
then enable fault injection only in that test sidecar:

```json
{
  "allow_fault_injection": true,
  "fault_injection": {
    "transform_orders": {"kind": "permanent", "attempts": [1]}
  }
}
```

This fragment belongs inside `runtime`; it is not a replacement for the full
schema. Run with `--config <test-sidecar.json>`. After failure, remove the
injection and resume the latest failed run. The two extracts should be reused,
the transform/downstream path should execute, and the report should contain
North 1500 / South 2050 cents with exactly one delivery intent.

Supported injected failures: `retryable`, `permanent`, `timeout` (optional
`delay_seconds`), `crash`, `after_output`, and `after_effect`. `crash` is allowed
only in the local subprocess runner; never kill a shared Spark driver as a child
fault. `after_effect` targets the sample delivery outbox, proving that a commit
before checkpoint acknowledgement does not duplicate the effect.

The automated matrix injects a failure into **every restartable sample node**.
For failure/timeout-handler nodes, their activating upstream failure is retained
while the handler is repaired; then the upstream is repaired and the full DAG
finishes. After a successful recovery, inactive failure handlers correctly become
`SKIPPED_CONDITION` rather than being forced to run.

Observed outcomes, including named matrix subtests and actual Spark checks, are
in `test-results.json`; native Windows portable outcomes are in
`test-results.windows.json`. These are reference-system results, not acceptance
evidence for an unprovided DataStage job or untested cloud workspace.

## Retention and cleanup

Protect all output/checkpoint versions referenced by recoverable runs and all
undelivered outbox references. A same-content recomputation can refresh a pending
outbox's output reference without creating another intent. Do not remove a
referenced artifact simply because its original physical run failed.

Only after the retention window and recovery/delivery obligations are satisfied
may an authorized retention process remove **specifically identified orphan
attempt paths**. The sample cleanup node intentionally deletes nothing.
Coordinate Delta OPTIMIZE/VACUUM, storage lifecycle rules, and control-history
archiving with the recovery SLA. Never recursively remove the deployment,
storage root, or control table as a routine cleanup action.
