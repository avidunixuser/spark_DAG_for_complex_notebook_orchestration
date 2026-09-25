# DataStage dependency assessment and conversion intake

## Evidence and boundary

The supplied material is a 515-line requirements specification. The original
repository contained only a two-line README naming Spark, Databricks, and
Fabric. **There is no evidence from which to reconstruct an actual DataStage
dependency graph or certify equivalent business results.**

The implementation therefore supplies a reusable runtime and an explicitly
synthetic sales DAG. It does not invent source job names, source SQL, business
rules, original restart points, original run results, or migration approval.
Actual conversion remains pending receipt of the following artifacts.

| Required input | What must be recovered and reviewed |
|---|---|
| DSX/ISX export, jobs, shared containers, sequences | Every stage/link, link ordering, sequencer AND/OR semantics, trigger expressions, nested job activities |
| Parameter sets and environment definitions, with secrets removed | Defaults, scopes, precedence, late-bound values, business-date rules |
| Transformer derivations and before/after routines | Null, decimal, date/time, collation, locale, surrogate key, and exception semantics |
| Server/PX job settings and generated code where available | Partitioning, sort guarantees, lookup cardinality, aggregation, buffering, abort thresholds |
| Shell scripts and external command specifications | Exit-code mapping, working directories, environment, file moves, database calls, side effects |
| Source/target metadata and SQL | Schemas, keys, CDC watermarks, isolation levels, overwrite/merge semantics, commit boundaries |
| Reject definitions and quality policies | Warning-versus-abort thresholds, rejected payload retention, remediation flow |
| Restart/checkpoint settings and representative logs | Completed activity semantics, reset behavior, manual interventions, retry and timeout history |
| Golden input/output snapshots and reconciliation rules | Record-level and aggregate correctness, restart equivalence, duplicate absence |
| Platform/security/capacity decisions | Runtime versions, identities, networking, quotas, source connection caps, retention, SLA |

Keep credentials and personal production data out of the repository and
conversation. Supply redacted metadata and representative synthetic fixtures.

## Target reference mapping matrix

These are **demonstration mappings, not claims about an original workload**.

| Sample lineage / DataStage analogue | Purpose, inputs, outputs | Predecessors / trigger | Spark mapping and persistence | Semantic difference / required validation |
|---|---|---|---|---|
| `SAMPLE::LandedOrders` / extraction activity | Landed order snapshot -> orders artifact | Entry / success | `extract_orders`; immutable committed artifact | Extraction is separate from processing. Choose Fabric Copy/Data Factory/CDC/export tooling after source assessment; not automatic JDBC |
| `SAMPLE::LandedCustomers` / parallel job activity | Landed customer snapshot -> customer artifact | Entry / success | `extract_customers`, parallel with orders | Source snapshot alignment and dimensional uniqueness need business approval |
| `SAMPLE::JoinAndReject` / transformer, lookup, duplicate removal | Orders + customers -> accepted/rejected artifacts | Both extracts / AND success barrier | `transform_orders`; DataFrame join, explicit validation, two-output checkpoint | Exact duplicates collapse; conflicting keys reject. Original duplicate, partition, null, sort, and lookup rules are unknown |
| `SAMPLE::QualityGate` / conditional/exception activity | Accepted/rejected counts -> approved reference | Transformation / success | `quality_gate`; read-only checkpoint | Default thresholds are sample-only, not inferred DataStage thresholds |
| `SAMPLE::AggregateAndPublish` / aggregate and target stage | Approved rows -> regional report | Quality gate / success | `publish_report`; exact integer cents and immutable report version | Downstream readers consume a committed output reference, not a partially overwritten live table |
| `SAMPLE::DeliveryOutbox` / shell delivery or notification | Report -> durable delivery intent | Publish / success and enabled parameter | `deliver_report`; business-key-deduplicated control-store outbox | No actual delivery is made. External consumer must honor the idempotency key or require manual reconciliation |
| `SAMPLE::FailureTrigger` / exception handler | Main-path states -> failure notice | All main nodes terminal / any failure or block | `notify_failure` | A handled critical failure still makes the run fail |
| `SAMPLE::TimeoutTrigger` / timeout trigger | Main-path states -> timeout notice | All main nodes terminal / any timeout | `notify_timeout` | Unconfirmed termination quarantines the run instead of launching potentially unsafe cleanup |
| `SAMPLE::CompletionTrigger` / after-job cleanup | Terminal branch states -> cleanup receipt | Delivery + handlers / completion | `cleanup`; read-only bookkeeping | Does not delete shared checkpoints or compensate unknown external effects |
| Unprovided routines, nested sequences, shell scripts | Unknown | Unknown | No invented implementation | Enumerate and map individually after intake; arbitrary shell execution is intentionally not exposed through JSON |

The reference supports `all_success`, `any_success`, `all_done`, `any_failed`,
and `any_timed_out`, plus parameter equality conditions without `eval`.
Failure/completion-triggered nodes can implement real cleanup/compensation in
the component registry. No generic framework can invent a correct business
compensation routine.

## Conversion procedure after intake

1. Inventory every job, stage, link, routine, script, parameter, external effect,
   and persisted dataset; attach exact source lineage to each node.
2. Reconstruct control dependencies and data dependencies separately. Identify
   conditional joins and rules for skipped branches; compare against run logs.
3. Select extraction/loading tools per source/target. Define immutable landed
   snapshots and control-store authority before translating transformations.
4. Group stages into logical components with explicit commit boundaries.
   Implement business functions separately from notebook/runtime code.
5. Classify every node's idempotency, restart eligibility, compensation/manual
   requirements, outputs, reject handling, deadlines, and retryable failures.
6. Replace the sample sidecar, notebooks, fixtures, manifest, and mapping rows.
   Review every semantic difference with the job owner.
7. Compare golden happy-path results and every failure/restart result, including
   record counts, exact keys/amounts, reject reasons, and external effects.
8. Run real cloud termination, locking, identity, throughput, and retention tests.
   Accept the conversion only after equivalence and operational sign-off.

## Open issues and assumptions

- Actual source lineage, business rules, source/target adapters, external
  compensation, secrets integration, and migration equivalence remain unknown.
- Business run keys are deliberately restricted to non-sensitive ISO dates in
  this version. Extending the strategy requires validation and new tests.
- The sample processes one complete landed snapshot per business key. It does
  not infer a partition filter, CDC watermark, or late-arrival policy.
- Cloud paths, imported notebook IDs/names, cluster settings, networking, and
  identities are intentionally unconfigured placeholders.
- ADLS Gen2 file-lease and cloud notebook APIs are contract-tested, not live-verified.
  Real cloud Delta transactions, session termination, and platform permissions
  must be acceptance-tested before production use.
- Infinite leases prioritize integrity over unattended recovery. A crashed
  writer requires confirmed quiescence and a controlled operator intervention.
- The outbox provides exactly-once **intent creation**, not exactly-once delivery
  to an arbitrary external system.
