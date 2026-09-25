# Runtime design, integrity, and capacity

## Deployment and configuration contract

`job_config.json` is the source of truth. `job_config.schema.json` uses JSON
Schema draft 2020-12 with schema version `1.1`; malformed JSON, duplicate keys,
non-finite numbers, missing fields, incompatible versions, unknown environments,
inline credential fields, and unconfigured cloud placeholders fail closed.

The shared loader resolves an explicit deployment root or absolute sidecar
path. With neither supplied, it checks the current directory, or the immediate
parent when running inside `notebooks`. It never searches arbitrary ancestors
and silently chooses a different deployment. An absolute sidecar must belong to
the explicitly supplied deployment root when both are specified.

Environment overrides merge objects recursively; arrays are **replaced**, never
positionally merged. The effective configuration is then validated. Schema
metadata cannot be overridden. JSON contains secret **references** only;
sample operations require no passwords. Azure lock authentication is explicit:
managed identity, workload identity, Fabric storage identity, or a development
credential chain. The latter is for local development, not production fallback.

Each notebook resolves/loads the sidecar through the same module, selects its
node definition, validates the runtime contract, and logs version plus SHA-256
fingerprint. Child work is rejected if the deployment changes after planning.
Business handlers receive only resolved parameters and upstream references.
No notebook contains an independent copy of environment settings.

Cloud locations are qualified against `lakehouse.root_uri`: control state under
`Tables`, artifacts/rejects under `Files`, and versioned Delta sources under
`Tables`. Canonical ABFSS paths also allow explicitly configured external
ADLS Gen2/Lakehouse locations. Coordination uses file-system/directory semantics
and the ADLS Gen2 Python SDK, including conditional file creation.

## DAG and scheduling

The validator rejects duplicate nodes/checkpoint names, missing definitions or
notebook source files, cycles, missing/duplicate dependencies, undefined entry
nodes, disconnected roots, unsupported triggers/restart policies, invalid
references, unsafe automatic retries, and invalid group/resource limits.
Lexicographic Kahn topological sorting gives a deterministic plan, recorded
before dispatch. The generated manifest documents the same logical graph.

Local and Databricks scheduling defaults to **eager dispatch**: a completed
attempt releases capacity and ready successors can start without waiting for an
unrelated slow branch. Queued, starting, and running attempts all consume their
reserved global/group/resource budgets until execution returns. Preparation is
bounded by available capacity instead of eagerly claiming every ready node.
`runtime.scheduling = "barrier"` preserves wave scheduling; Fabric requires it
and executes each wave through native shared-session `runMultiple`. Fan-in waits for every
dependency to become terminal, including any configured retries. A success-only
node blocked by a failed dependency becomes `BLOCKED`; an inactive conditional
branch propagates `SKIPPED_CONDITION`, not a false success. `any_success` can join
alternative branches, while `all_done` enables cleanup after either outcome.

Retries apply only to configured categories, a retryable child result, remaining
attempt budget, confirmed termination, restart eligibility, and a safe
idempotency class. Delay is bounded exponential backoff:
`min(max_delay_seconds, delay_seconds * backoff ** (failed_attempt - 1))`.
Native Fabric/Databricks retries are disabled to avoid a second retry authority.
The orchestrator enforces the retry due time before dispatch. A child claim uses
the atomic state and attempt token, not a comparison with a different driver's
wall clock. Synchronize platform clocks for the absolute commit deadlines.

`continue_independent` allows unrelated work and failure handlers to complete.
`fail_fast` cancels unscheduled success-path work after an exhausted critical
failure, while allowing failure/completion handlers. An uncertain worker stops
new dispatch entirely, including cleanup that might race its side effects.

## State machine

```text
PENDING -> READY -> RUNNING -> SUCCEEDED
                         \-> FAILED --------> READY (eligible retry only)
                         \-> TIMED_OUT -----> READY (confirmed stopped only)
PENDING -> SKIPPED_ALREADY_SATISFIED (proven checkpoint/output reuse)
PENDING -> SKIPPED_CONDITION
PENDING -> BLOCKED
PENDING/READY -> CANCELLED

RUNNING/RECOVERY_REQUIRED run
    -> explicit confirmed-stopped reconciliation -> FAILED -> new resume run
```

Each recovery is a new physical run with its own UUID and attempt number and a
parent run ID; the business run key defines the logical workload. Only the
latest failed physical run can be resumed. Old successful branches can carry
their original provenance through multiple recoveries.

Normal mode refuses a business key with history. Resume reruns invalid/failed/
timed-out/incomplete nodes and eligible descendants. Force restart adds the
selected node or checkpoint to that invalidation closure. Force rerun invalidates
the entire graph but still obeys branch conditions and non-idempotent guards.
Modes and reasons remain distinct in configuration, parameters, events, and
summaries.

## Transactional control store

The physical Delta table has:

| Column | Meaning |
|---|---|
| `scope STRING` | SHA-256 of application ID, DAG ID, and business key |
| `sequence LONG` | Monotonic sequence within that scope |
| `entity_type STRING` | `run`, `node`, or `outbox` |
| `entity_id STRING` | Run UUID, run/node identity, or stable effect key |
| `run_id`, `node_id` | Direct audit filtering/lineage |
| `event_type`, `timestamp` | Transition name and UTC event time |
| `payload STRING` | JSON snapshot of the entity **after** the transition |

The latest event for an entity is its current state. A state update and its
audit trail are **one record in one Delta transaction**, not two independently
committed tables. Run initialization batches the run and all PENDING nodes in
one append. Duplicate/gapped scope sequences are rejected on read.

The payload tracks application/DAG/code/configuration versions, run and parent
IDs, business key, physical attempt, node/notebook/original lineage, dependency
states, start/end/duration, input fingerprint, outputs/checkpoint, retry count,
sanitized error/category, metrics/counts, restart/rerun reason, criticality,
remote run ID, and uncertainty. Skips record provenance rather than pretending
to have executed an attempt.

Delta's informational keys are not unique constraints. The runtime therefore
uses **two ADLS Gen2 file-lease namespaces**:

- A business-scope run lease excludes overlapping orchestrators.
- A short-held scope/control-table lease serializes each read/compare/append
  transaction across orchestrator and child notebooks. Initialization has a
  separate table-path lease.

All leases have duration `-1` (infinite). They cannot silently expire during a
long Spark commit. The owner checks its lease before writes; a lost or
unverifiable lock stops progress. An uncertain Delta append retains its mutex
instead of retrying a potentially committed append. A crashed/uncertain run
retains its run lease. **No automated lease breaking or stale-heartbeat reclaim
is implemented.** Operators must prove all writers stopped before breaking one.

Every writer must use this protocol, the same DFS account/file-system/directory,
and the same canonical control-table path. Direct table writes, aliases for the
same table, or manual lease breaking while a writer is alive invalidate the
guarantees. Protect the table and coordination directory accordingly. Lock-file
creation uses `If-None-Match: *`; a competing owner never has its file replaced.

Each Delta backend maintains an attempt/run-scoped append-only history cache.
Reads always fetch the suffix after its last committed sequence, using Delta
data skipping, and check sequence continuity before extending that cache.
Committed local appends are remembered only after the write acknowledges.
Starting another run clears that scope's cache while holding the run lease.
In-place control-event modification or deletion is unsupported; archive only
closed, expired scopes with no active drivers.

SQLite implements the same append/projection contract using `BEGIN IMMEDIATE`
and a `(scope, sequence)` primary key. A persistent RUNNING record blocks another
local orchestrator even after process death. SQLite/WAL is not supported on
OneLake, DBFS, object-store mounts, network shares, or multiple cloud drivers.

## Child notebook contract

The platform parameter `context_json` is an ASCII JSON envelope containing:

```text
run_id, business_run_key, node_id, attempt, restart_mode,
config_path, deployment_dir, environment, scope, claim_id,
input_fingerprint, configuration_fingerprint, code_fingerprint, deadline,
parent_context, upstream_outputs, dependency_status, parameters
```

The child independently validates configuration, identity, parent context,
inputs, and its expected component; atomically claims READY -> RUNNING using
the exact attempt token; executes; validates committed outputs; and atomically
records its final checkpoint and terminal state. Exceptions become persisted
sanitized failure records and are raised to the notebook caller. An unavailable
control store is not disguised as a successful result.

The structured result includes node/run IDs, attempt, status, start/end,
duration, output references, metrics, row counts, warning/reject counts,
retryability, and sanitized error data. The orchestrator treats the durable
control record, not an untrusted exit string, as the completion authority.

## Skip proofs and idempotency

Reuse requires the same logical business key and DAG, a compatible declared DAG
version, identical deployed code fingerprint/version, the same relevant input/
business-configuration fingerprint, restart eligibility, a reuse-safe strategy,
valid dependency states, existing validated outputs, a matching committed
checkpoint, and no explicit invalidation.

Fingerprints include resolved business parameters, source snapshots, dependency
states/output digests, Spark settings, storage destinations, component definition,
checkpoint contract, and deployed Python/notebook contents. Local CSV sources
are hashed; Delta inputs use table identity, explicit version, and snapshot
schema rather than repeatedly scanning source rows. Delta data/log files must
not be edited in place outside the transaction protocol. Retry, fault-injection, and
logging changes do not invalidate unrelated business outputs. An actual source
or relevant rule change does. Source snapshots must remain immutable throughout
an attempt.

Supported classes: natural, overwrite, merge, business-key deduplication,
committed checkpoint, compensation-required, and manual-approval-required.
The engine validates the policy but cannot prove a custom handler implements
the declared strategy. Non-idempotent work never automatically retries.
Recovery requires explicit node approval; compensation-class nodes additionally
require a validated successful companion checkpoint.

Local artifacts use fsync plus same-filesystem atomic rename. Cloud artifacts
use immutable per-attempt Delta paths and pinned versions. Multi-output
components advertise nothing until all outputs pass validation. A partial write
is an unreferenced orphan, not a success. Failed-attempt artifacts are never
passed downstream. Checkpoints and output references become visible together
in the terminal control event.

Delta output validation checks table identity, pinned version, schema, row count,
and a distributed content digest (SHA-256 row hashes aggregated in four decimal
sums plus count). Row ordering is not semantic. Retain files/log versions for at
least the promised restart horizon; VACUUM and lifecycle policies must not
invalidate live checkpoints.

Within a child attempt, a pinned Delta output's completed validation proof can
be reused for its read and final checkpoint. The proof key includes the entire
reference, and the cache is cleared at each child entry. Restart/skip validation
remains fresh, and an explicit `validate()` call always rechecks the artifact.
Mutable local files and outbox records are never accepted through this cache.

Outbox intent creation uses a stable application/DAG/business-key/node identity
and payload digest under the control mutex. An identical retry returns the same
receipt. Different payload under the same delivery identity is a **business
conflict**, not a silent replacement. A consumer must send the same idempotency
key to a receiver that deduplicates; otherwise use reconciliation/approval.

## Platform timeout differences

| Executor | Execution and cancellation boundary |
|---|---|
| Local | Separate OS process per child; timeout kills and reaps it before retry. Local handlers must not spawn detached workers |
| Databricks | Jobs API submission with stable idempotency token, persisted remote run ID, explicit polling, task timeout, cancel, and terminal confirmation. Network ambiguity or unconfirmed cancellation quarantines the attempt |
| Fabric | `notebookutils.notebook.runMultiple` executes each ready wave in the shared Spark session. Per-cell and wave deadlines are native; an absolute child deadline also prevents late success commits. No supported per-child hard-cancel proof is assumed. An unacknowledged timeout/exception requires session reconciliation |

Fabric's per-cell timeout is **not** a precise whole-notebook deadline. This
implementation enforces the commit deadline and refuses unsafe automatic replay
when termination is unknown, rather than pretending a cancelled Python Future
stopped Spark. All native notebook retries are zero. Fabric child notebooks
share the parent's lakehouse/session; they must not mutate session-wide settings,
use global temporary views for exchange, stop the session, or launch background
writes.

## Observability and performance

Structured events contain run/business/DAG/node identities, attempt, transition,
timestamp, duration, configuration/code version, retry count, and error category.
Only allowlisted metadata is logged. Raw exceptions, parameter values, data
rows, credentials, and response bodies are not logged. Business keys are
non-sensitive dates; disable their logging if organizational policy requires it.
Protect control payloads, output paths, rejects, and native platform diagnostics
with appropriate ACLs.

Summaries partition node outcomes, count retried nodes, report total duration,
restart mode/origin/reasons, failures and operator action. Critical-path duration
is the longest dependency path weighted by observed attempt work time; it
excludes queue/backoff and is not an estimate of optimal cluster runtime.

Capacity planning must consider:

- Eager scheduling overlaps independent chains while retaining dependency
  barriers and all resource limits. Fabric's native wave boundary remains a
  platform constraint, not a claim of dynamic shared-session cancellation.
- Limits are per orchestrator run. Set the outer Fabric pipeline/Databricks job
  admission limit to one, or supply separate global admission control, when
  multiple business keys share a source/target quota.
- Native Databricks child job startup can dominate small components. Use a
  correctly sized existing cluster and group fine-grained DataStage stages into
  logical notebooks. Fabric waves share driver/executor resources.
- Each Delta transition is still a small transaction. Incremental scope reads
  avoid repeated full-history transfers, but the projection remains in driver
  memory; do not use an unbounded business key for years
  of reruns. Archive closed scopes and benchmark expected DAG size/retry volume.
- Full output validation adds I/O; producing attempts reuse completed proofs
  rather than hashing the same committed artifact again. A shared, disk-backed
  Spark classification cache avoids recomputing the order join for accepted and
  reject outputs and is released in `finally`, including failure paths.
- DAG indexes are reused, descendant traversal is linear in vertices/edges,
  and topological sorting uses adjacency lists rather than repeated all-node scans.
- The Python local runner collects data and is for small fixtures only. Cloud
  transformations use DataFrames; do not collect production datasets.

## Verified API references

- [Fabric notebook run and orchestration](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-notebook-run)
- [Databricks notebook orchestration](https://learn.microsoft.com/azure/databricks/notebooks/notebook-workflows)
- [Databricks Jobs SDK/API surface](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html)
- [ADLS Gen2 DataLakeLeaseClient](https://learn.microsoft.com/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakeleaseclient)
- [OneLake API parity and managed folders](https://learn.microsoft.com/fabric/onelake/onelake-api-parity)
- [Databricks constraints](https://learn.microsoft.com/azure/databricks/tables/constraints)
