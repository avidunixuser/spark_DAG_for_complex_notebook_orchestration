# Restartable Spark notebook DAG

A portable reference runtime for **Microsoft Fabric**, **Databricks**, and local
failure/recovery testing. The sample is a **supply-chain warehouse receiving
workflow** driven by batches of canonical XML files, with business validation
separate from orchestration. Cloud state and data use **Lakehouse Delta tables and ADLS Gen2
paths**, coordinated by **ADLS Gen2 file leases**. Local runs use transactional
SQLite.

**This is not a completed DataStage conversion.** No DataStage export, sequence,
routine, script, or source-system specification has been supplied. The sample
lineage is explicitly labeled `SAMPLE::...`. See [the assessment](assessment.md)
for the artifacts needed to perform the actual conversion. Cloud adapters need
deployment-specific acceptance testing; no cloud resources are provisioned here.

## Run the reference DAG

From this deployment folder, in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test,cloud]"
.\.venv\Scripts\python.exe -m spark_dag validate
.\.venv\Scripts\python.exe -m spark_dag run --business-run-key 2026-09-24
```

The local executor launches real child processes and terminates/reaps them on
timeout. It uses the same configuration, business components, state machine,
checkpoint validation, and retry policies as the notebook entry points, but uses
Python reference transformations rather than requiring Spark. It is a test
runner, **not a distributed SQLite deployment option**.

The canonical input is **six XML files**: four shipment batches and two product
catalog files under `sample_data`. Each shipment line carries an ASN/shipment ID,
line ID, SKU, destination warehouse, quantity, and unit of measure.

The expected **warehouse receiving plan** is:

| Warehouse | Product | Expected receipt | Accepted shipment lines |
|---|---|---:|---:|
| `WH-ATL` | `SKU-FILTER` - replacement air filters | **120 EA** | **2** |
| `WH-DFW` | `SKU-SEAL` - hydraulic seal kits | **60 EA** | **1** |

One retransmitted shipment line is collapsed using its **shipment ID + line ID**.
An unknown SKU and a negative quantity are quarantined, with original line fields
and reject reasons preserved. The final step creates one deduplicated
**inventory-receipt outbox intent**; it does **not** post stock to a warehouse
management system (WMS). Local committed outputs, rejects, and control state are
under `.runtime`. See [the canonical XML contract](canonical-input.md) and
[the expected plan fixture](sample_data/expected_receiving_plan.json).

## Architecture

### Animated walkthrough

Watch failed shipment validation recover without repeating valid XML ingestion:

<picture>
  <source media="(prefers-reduced-motion: reduce)" srcset="docs/dag-poster.png">
  <img src="docs/dag-animation.gif" alt="Recorded warehouse DAG animation: XML shipment and product ingestion succeeds, shipment validation fails, and a resumed run reuses ingestion checkpoints before completing the receiving plan.">
</picture>

[Static image](docs/dag-poster.png) |
[Interactive player](docs/dag-player.html) |
[Recorded events](docs/dag-recording.json)

The GIF shows **failure and resume**. The interactive player adds **happy path,
transient retry, and timeout/resume** scenarios, with play/pause, previous/next
event, a timeline scrubber, and speed controls. It starts paused, respects
reduced-motion preferences, and includes an accessible node-state table.

GitHub displays the GIF but does not execute HTML players inside a README.
After cloning, open `docs/dag-player.html` directly in a browser, or serve only
the documentation folder:

```powershell
python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

Then visit <http://127.0.0.1:8765/dag-player.html>. The player is self-contained:
no CDN, external fonts, network access, cloud credentials, or telemetry.

These are **recorded local sample executions, not live production monitoring**.
The recorder preserves committed audit-event order, verifies the receiving plan
and single inventory-receipt intent, and publishes only allowlisted metadata. Playback
is paced for readability, not representative of Spark execution time.

Regenerate the views from the committed recording:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[animation]"
.\.venv\Scripts\python.exe tools\dag_animation.py
```

Add `--record` to capture new isolated local sample runs first; this includes
intentional failures and timeout termination, never a cloud deployment. The
generator refuses non-reference configurations. The recorded event file,
GIF, static image, and player are generated together in `docs`.

Recording/media checks run in the regular test suite. The `playback` CI job
also exercises the controls in Chromium; run it locally with the `browser-test`
extra, `python -m playwright install chromium`, and
`SPARK_DAG_BROWSER_TESTS=1` when invoking `python -m unittest tests.test_dag_player`.

### Runtime and storage

The orchestrator selects one execution adapter per run. All child notebooks
reload the shared configuration and use the same claim, validation, and
checkpoint contract; local subprocesses execute the corresponding Python
components. This view shows runtime relationships, not additional DAG nodes.

```mermaid
flowchart TB
    config["job_config.json<br/>job_config.schema.json"]
    validation["Shared configuration loader<br/>Orchestrator DAG validation"]
    orchestrator["orchestrator.ipynb<br/>Dependencies, resource limits, retries, recovery"]

    config --> validation --> orchestrator

    subgraph adapters["Execution adapters - one per run"]
        direction LR
        local["Local subprocesses<br/>Eager scheduling"]
        databricks["Databricks Jobs API<br/>Eager scheduling"]
        fabric["Fabric runMultiple<br/>Shared Spark session and barriers"]
    end

    orchestrator --> local
    orchestrator --> databricks
    orchestrator --> fabric
    local --> components["Logical DAG components<br/>Cloud notebooks or local workers"]
    databricks --> components
    fabric --> components
    config -. "Reloaded by each child" .-> components

    sources[("Canonical XML file sets<br/>Shipment batches and product catalogs")]
    outputs[("Committed outputs and rejects<br/>Lakehouse Files or local artifacts")]
    control[("Control store<br/>Lakehouse Delta in cloud<br/>SQLite locally")]
    leases["ADLS Gen2 file leases<br/>Cloud coordination only"]
    outbox["Deduplicated inventory-receipt intent<br/>No WMS stock posting in the reference"]

    sources --> components --> outputs
    orchestrator <-->|Run state and recovery| control
    components <-->|Attempt claims, checkpoints, outcomes| control
    control --> outbox
    leases -. "Exclude overlapping cloud runs" .-> orchestrator
    leases -. "Serialize cloud control commits" .-> control
```

### Processing DAG

The nine notebook nodes below match `job_config.json`. Solid edges inside the
main path are success dependencies; dotted edges report terminal states to
the orchestrator's trigger evaluation.

```mermaid
flowchart TB
    subgraph main_path["Main processing path"]
        direction TB
        extract_shipments["extract_shipments<br/>Read canonical XML shipment batches"]
        extract_products["extract_products<br/>Read canonical XML product catalogs"]
        validate_shipments["validate_shipments<br/>Validate keys, SKU, units and quantity"]
        quality_gate["quality_gate<br/>Accepted-row and reject thresholds"]
        plan_receipts["plan_receipts<br/>Expected units by warehouse and SKU"]
        request_receipts["request_receipts<br/>Inventory-receipt intent when enabled"]

        extract_shipments --> validate_shipments
        extract_products --> validate_shipments
        validate_shipments --> quality_gate --> plan_receipts --> request_receipts
    end

    rejects[("Rejected rows<br/>Durable quarantine artifacts")]
    terminal{"All six main-path nodes terminal<br/>Orchestrator trigger evaluation"}
    notify_failure["notify_failure<br/>any_failed"]
    notify_timeout["notify_timeout<br/>any_timed_out"]
    cleanup["cleanup<br/>all_done: completion bookkeeping"]

    validate_shipments -->|Rejected rows| rejects
    extract_shipments -.-> terminal
    extract_products -.-> terminal
    validate_shipments -.-> terminal
    quality_gate -.-> terminal
    plan_receipts -.-> terminal
    request_receipts -.-> terminal
    terminal -->|Failure condition| notify_failure
    terminal -->|Timeout condition| notify_timeout
    request_receipts --> cleanup
    notify_failure --> cleanup
    notify_timeout --> cleanup
```

The terminal-state diamond is a logical barrier, **not another notebook**.
`targets.inventory_receipts.enabled` controls the receipt-intent branch.
Inactive notice branches become `SKIPPED_CONDITION`; cleanup waits for receipt intent
and both notices to reach terminal states. An unconfirmed cloud termination
instead stops further dispatch for operator reconciliation. Cleanup records
bookkeeping and does not delete shared data or checkpoints.

## Recovery

Use the failed run ID from the JSON summary:

```powershell
.\.venv\Scripts\python.exe -m spark_dag run --business-run-key 2026-09-24 --mode resume --parent-run-id "<failed-run-id>" --reason INC-123
.\.venv\Scripts\python.exe -m spark_dag run --business-run-key 2026-09-24 --mode force_restart --parent-run-id "<latest-run-id>" --restart-from validate_shipments --reason CHG-124
.\.venv\Scripts\python.exe -m spark_dag run --business-run-key 2026-09-24 --mode force_rerun --reason CHG-125
```

**Force restart** invalidates the selected node/checkpoint and its descendants.
**Force rerun** repeats every currently eligible node, but does not bypass
non-idempotent approval, compensation, or side-effect deduplication.
An already-used business key is rejected in normal mode. Configuration/code,
dependency state, input snapshots, outputs, and final checkpoints are checked
before any `SKIPPED_ALREADY_SATISFIED` result.

## Deployment folder

| Artifact | Purpose |
|---|---|
| `orchestrator.ipynb` | Parameterized orchestrator entry point |
| `notebooks/*.ipynb` | One entry point per logical sample component |
| `child_template.ipynb` | Contract-preserving starting point for a real converted component |
| `job_config.json` / `job_config.schema.json` | Sidecar and strict versioned schema |
| `dag_manifest.json` | Generated lineage, policies, edges, and deterministic plan |
| `docs/dag-player.html` / `docs/dag-animation.gif` | Offline interactive playback and README animation |
| `docs/dag-recording.json` / `tools/dag_animation.py` | Sanitized sample audit recording and reproducible media generator |
| `spark_dag/` | Loader, validator, scheduler, control store, leases, adapters, artifacts, business logic |
| `sample_data/` | Six synthetic canonical XML files, the XSD contract, and expected receiving plan |
| [canonical-input.md](canonical-input.md) | XML envelopes, field rules, limits, and receiving semantics |
| `tests/` / `tools/run_tests.py` | Configuration, DAG, concurrency, recovery, adapter, and Spark checks |
| `test-results.json` | Recorded results and explicit validation scope |
| [validation.md](validation.md) | Observed results, failure-injection matrix, and requirement coverage |
| [performance.md](performance.md) | Scheduling benchmark, I/O reductions, and capacity constraints |
| [design.md](design.md) | Contracts, state/control design, integrity, capacity, and platform differences |
| [deployment.md](deployment.md) | Local, Fabric, and Databricks deployment steps |
| [runbook.md](runbook.md) | Failure injection, restart, quarantine, and operator recovery |
| [assessment.md](assessment.md) | DataStage intake, sample mapping, assumptions, and unresolved decisions |

The sidecar lives at the **deployment root**, shared by the orchestrator and the
`notebooks` subfolder. Every notebook uses the same loader; it never embeds its
own environment configuration. A different sidecar filename or an absolute
resolved path can be passed at runtime. All hosts must see the same immutable
deployment release.

## Automated checks

```powershell
.\.venv\Scripts\python.exe -m ruff check spark_dag tests tools
.\.venv\Scripts\python.exe tools\run_tests.py --output test-results.json
```

Real PySpark checks additionally require **Java 17 or 21**, the declared
`spark-test` extra, and a working local Spark environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[test,cloud,spark-test]"
.\.venv\Scripts\python.exe tools\run_tests.py --spark --delta --output test-results.json
```

The `--spark` flag makes Spark setup/test failures fail the suite rather than
quietly skipping them. Without it, Spark tests are explicitly reported as
skipped. `--delta` additionally exercises real Delta transactions and immutable
output validation; it needs Maven access for the declared Delta jars. Unit
tests exercise cloud API contracts using fakes; this is not proof
of live Fabric, Databricks, OneLake, Delta-on-cloud, or ADLS Gen2 lease behavior.
GitHub Actions runs portable checks on Windows/Linux and real Spark checks on
Linux.

Schema **1.2** adds bounded canonical XML input contracts. ADLS Gen2 coordination
uses `file_system` and `directory`; cloud paths resolve against `lakehouse.root_uri`.
Local/Databricks runs default to
resource-bounded eager scheduling; Fabric retains native shared-session
`runMultiple` barriers. See the migration section in [deployment.md](deployment.md)
before replacing an existing coordination namespace.
