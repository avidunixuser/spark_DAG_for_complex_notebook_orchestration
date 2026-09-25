# Deployment instructions

## Release contract

Deploy **one immutable release folder** containing the sidecar and schema,
orchestrator/child notebook sources, manifest, package or wheel, sample/landed
input definitions, tests, and operator documents. The executable notebooks may
be platform items, but their `.ipynb` source files must also remain in the shared
release folder: validation and code fingerprints use those sources.

Install the `spark_dag` package on every notebook driver before running the
orchestrator. A wheel contains Python modules, not the sidecar/notebook assets.

```powershell
python -m pip wheel . --no-deps --wheel-dir dist
```

Never edit the deployed release while a run is active. Publish a new version,
remediate/reconcile the current run if necessary, and start an explicit recovery
against that version.

## Local reference deployment

Use Python 3.11 or newer and a local filesystem supporting SQLite and atomic
rename. Install the project and run the commands in the README. The CLI accepts
`--config` and `--deployment-dir` on every subcommand.

No Spark, Azure account, cloud credentials, JDBC driver, or external target is
needed for the local sample. Its subprocess executor exercises genuine process
timeouts. Do not put SQLite control files on network/object-store mounts.

The optional real Spark tests are best run on Linux with Python 3.11/3.12 and
Java 17/21. Java 25 cannot start this Spark 4.0.1/Hadoop combination. Native
Windows Python 3.13 can run the portable suite, but the available Spark Python
worker failed with `WinError 10038` in this environment. A supported Linux
environment avoids treating that host limitation as a business-logic result.

## Shared cloud prerequisites

| Requirement | Setup/validation |
|---|---|
| Spark and Delta | Use the platform's bundled compatible Spark/Delta versions. Do **not** install the local `spark-test` extra into a managed notebook runtime |
| Package dependencies | Install this wheel plus `jsonschema`, `azure-identity`, `azure-storage-file-datalake`; Databricks also uses `databricks-sdk` |
| Shared release filesystem | Every child driver can read the same absolute deployment folder and sidecar |
| Delta storage | Separate, access-controlled locations for control events, immutable outputs, and rejects; avoid using the same physical directory for any two |
| ADLS Gen2 coordination | Existing HNS-enabled account, file system, and private coordination directory; identity can conditionally create files and acquire/renew/release file leases |
| Identity | Managed identity where supported; deterministic workload identity for federated jobs; Fabric's notebook storage token only where it has the required ADLS data-plane access |
| Networking | Access to Lakehouse data/control storage, the ADLS DFS endpoint and SDK-required storage endpoints, and Databricks Jobs API where applicable |
| Admissions | Limit outer orchestrator job/pipeline concurrency to one if resource budgets are shared across business keys; turn off blind outer-job retries |
| Lifecycle | Retain Delta data/log versions, rejects, and control history for the required recovery horizon |

Assign least privilege at the file-system/directory scope. Do not put storage keys,
SAS URLs, PATs, client secrets, or connection strings in JSON. SDK authentication
must use the platform-approved identity configuration. Keep SDK HTTP debug
logging disabled. An identity that can access OneLake is **not automatically**
authorized on the separate ADLS Gen2 coordination account. Set directory ACLs
and the appropriate storage data-plane role for that identity.

Application code uses `DataLakeServiceClient`, `DataLakeFileClient`, and
`DataLakeLeaseClient`, not object/container clients. ADLS Gen2 is built on Azure
Storage; the vendor SDK can use underlying storage transports for leases.
Do not assume that permitting only one endpoint is sufficient for private
networking. Validate the actual SDK traffic and permissions in your deployment.

Select `fabric` or `databricks` via the orchestrator's `environment` parameter.
Replace **every** `__REQUIRED__` in the selected override. Unknown/unconfigured
environments fail validation. Configure:

- `lakehouse.root_uri`: an ABFSS Lakehouse root. For Fabric, use
  `abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>`.
  For an ADLS-backed Databricks Lakehouse, use
  `abfss://<file-system>@<account>.dfs.core.windows.net/<lakehouse-directory>`.
- `control_store.path`: defaults to `Tables/dag_control_events` under that root.
  Keep one canonical control location for every writer.
- `storage.path` and `reject_data.path`: default to `Files/dag/artifacts` and
  `Files/dag/rejects`. Relative paths are qualified against the Lakehouse root;
  explicit canonical ABFSS locations are also supported.
- `sources.*.path` and `snapshot_version`: specific landed Delta snapshots.
- `locking.account_url`, `file_system`, `directory`, `credential`, and optional
  managed identity client ID. The endpoint must be HTTPS/DFS, e.g.
  `https://<account>.dfs.core.windows.net`. Precreate the file system and
  coordination directory and use the same namespace for every writer.
- Platform notebook names/base path and cluster/workspace settings.
- Every node's timeout/retry/resource budget and the overall run timeout.
  The small local sample timeouts are not production sizing recommendations.

For schema-enabled Fabric Lakehouses, configure paths such as
`Tables/dbo/dag_control_events` and `Tables/dbo/orders` as appropriate.
OneLake uses the workspace as its file-system address and the Lakehouse item
as the next path segment. Do not create or lease the workspace, item, `Files`,
or `Tables` managed roots. If coordinating directly through a validated OneLake
endpoint, put lock files below `<lakehouse-id>/Files/<coordination-directory>`;
certify file-lease/conditional-create behavior first. The supplied cloud examples
use a separate ADLS Gen2 coordination directory and Lakehouse data paths.

## Upgrade from schema 1.0

1. Disable all old schedules and prove old orchestrators, notebooks, and external
   side effects have stopped. Changing the coordination endpoint or directory
   while old writers are alive would create independent lock namespaces.
2. Preserve the existing Delta control history. Do not reset the control table
   or business-key history to make a migration pass.
3. Use an HNS-enabled ADLS Gen2 account; replacing a hostname is not an HNS
   migration. Replace `locking.container` with `file_system`, `locking.prefix`
   with `directory`, and configure the DFS endpoint.
4. Set `schema_version` to `1.1`, add `lakehouse.root_uri`, and configure
   `runtime.scheduling`: `eager` for local/Databricks or `barrier` for Fabric.
   The loader rejects the old schema rather than silently translating it.
5. Deploy the new immutable release and run the acceptance checklist. Changed
   code fingerprints invalidate prior reuse proofs; non-idempotent nodes still
   require explicit approval/compensation. Resume only from a reconciled latest
   failed run, or use an explicitly authorized force mode.

### Seed the synthetic Delta inputs

After choosing **new empty sample source locations**, the following optional
setup uses the sample CSVs. It intentionally refuses to overwrite an existing
table:

```python
from spark_dag.config_loader import load_config

config = load_config(config_file="job_config.json", deployment_dir=deployment_dir,
                     environment=environment)
for name, source in config.data["sources"].items():
    csv_path = str(config.root / "sample_data" / f"{name}.csv")
    frame = spark.read.schema(source["schema"]).option("header", True).csv(csv_path)
    frame.write.format("delta").mode("errorifexists").save(source["path"])
```

Fresh seed tables start at version 0. Confirm their actual schema/version and
set `snapshot_version` accordingly. For a real migration, replace this step with
the approved landing/CDC/data-movement process; do not silently overwrite source
snapshots used by earlier runs.

## Microsoft Fabric

1. Publish the package/dependencies through a Fabric Environment and attach it
   to the orchestrator and every child notebook. Ensure the modules are
   importable before running any cell.
2. Put the release assets in a common lakehouse Files folder or other approved
   readable filesystem mount. Supply its absolute mounted path as
   `deployment_dir`; a Fabric notebook item name is not a filesystem path.
3. Import `orchestrator.ipynb` and each child `.ipynb` as notebook items with
   names matching `notebooks.*.fabric_name`. Verify the first code cell is
   marked **parameters** after import. The generator supplies the tag.
4. Attach the same default lakehouse as the parent (or inherit it). This runtime
   does not bypass Fabric's child/parent lakehouse compatibility check.
5. Set the cloud overrides and ADLS coordination identity. `fabric_user` obtains a
   storage token through `notebookutils.credentials.getToken("storage")`;
   verify its identity and file-system/directory permissions in the scheduled context.
6. Supply orchestrator parameters: `deployment_dir`, `config_file`,
   `environment`, `business_run_key`, `restart_mode`, `parent_run_id`,
   `restart_from`, `reason`, and `approved_nodes_json`.
7. Run the acceptance checklist below in a dedicated test workspace before
   enabling any production schedule.

Ready waves use `notebookutils.notebook.runMultiple`, with native retries
disabled. Child business execution is one code cell; exit is in a separate cell
outside exception handlers. Per-cell and wave limits plus an absolute commit
deadline do not constitute proof of hard termination of all Spark side effects.
Read the quarantine procedure before testing timeouts.

## Databricks

1. Install the wheel and cloud dependencies as cluster libraries on an existing
   suitable cluster. Configure supported workload identity for the ADLS coordination
   service and the Databricks SDK's approved workspace authentication.
2. Copy release assets to a shared volume or supported workspace filesystem
   location. All child drivers must resolve the same `deployment_dir`.
3. Import the notebooks into a workspace folder. Set `notebook_base_path` to
   the absolute **workspace notebook folder**, e.g. a folder below `/Shared`,
   not the physical volume path. Child names are the source filename stems.
4. Set `runtime.databricks.existing_cluster_id`. The execution identity needs
   permission to attach to that cluster and submit/read/cancel job runs.
5. Run the orchestrator notebook as the outer scheduled job. Configure its
   notebook parameters/widgets and limit outer concurrency as required.
   `dbutils.widgets.getAll` must be available on the selected runtime.
6. Certify deadlines including job queue/startup time, cancellation convergence,
   cluster policies, and library availability on each ephemeral child run.

The adapter uses Jobs API 2.1 `runs/submit`, `runs/get`, and `runs/cancel` through
the authenticated SDK API client. It does not assume `dbutils.notebook.run`
returns a cancellation handle or provides Fabric-style shared-session DAG
execution.

## Acceptance and release gates

Run each check on **both target platforms actually being released**:

| Gate | Required evidence |
|---|---|
| Configuration and deployment | Every notebook finds exactly the selected sidecar; schema/fingerprint matches; missing/unconfigured assets fail before dispatch |
| Happy path / fan-out / fan-in | Correct report/rejects, bounded concurrency, no downstream start before its barrier |
| Every restartable node | Inject failure; remediate; prove valid successes are reused and eligible descendants complete |
| Timeout / cancellation | Prove child termination where supported; otherwise obtain `RECOVERY_REQUIRED`, no retry, no new unsafe dispatch |
| Lease collision / crash | Second orchestrator cannot claim; crashed writer is not automatically replaced; recovery follows the runbook |
| Delta control integrity | Atomic state/audit commit, monotonically ordered scope events, no duplicate claims under contention |
| Side effects | Exactly one intent for identical content, explicit conflict for changed content, receiver deduplication proven separately |
| Version / source changes | Relevant changes invalidate proofs; irrelevant retry/logging changes do not invalidate unrelated outputs |
| Retention and scale | Required versioned outputs survive the restart window; measured source/target caps and SLA are met |
| Real DataStage equivalence | Golden input/output and operational behavior match the supplied original job; owner signs off |

Do not call the reference implementation production-certified or the DataStage
conversion complete on the basis of local/mocked checks alone.
