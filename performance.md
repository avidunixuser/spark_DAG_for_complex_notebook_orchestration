# Performance and capacity

## Measured scheduling behavior

`tools/benchmark_dag.py` compares **the same current runtime** in barrier and
eager modes, with identical synthetic data, two child slots, resource limits,
output validation, and one deduplicated delivery intent. It adds an independent
successor to the fast extraction branch and gives the slow branch and successor
0.5 seconds of synthetic work each.

The three-repeat local measurement for the current runtime was:

| Scheduling | Median elapsed time |
|---|---:|
| Barrier | 1.9357 seconds |
| Eager | 1.3180 seconds |

That is approximately **32% less elapsed time**, or **1.47x speedup**, for this
small scenario. Eager mode started the fast branch's successor before the
unrelated slow branch finished; barrier mode did not. Every run produced the
same report and exactly one delivery intent. The full observations, runtime,
fingerprints, and synthetic delays are in [performance-results.json](performance-results.json).

These are **local scheduling measurements**, not a Fabric/Databricks throughput
claim or SLA. They isolate the scheduling change; both modes already include
the I/O optimizations below. Dataset size, platform startup, network latency,
identity, Spark capacity, and storage contention still require cloud benchmarks.

```powershell
.\.venv\Scripts\python.exe tools\benchmark_dag.py --repetitions 3 --output performance-results.json
```

## Changes that reduce overhead

| Area | Implementation and verified contract |
|---|---|
| Scheduling | Eager local/Databricks dispatch releases a slot after each confirmed completed attempt; successors no longer wait for unrelated branches |
| Resource admission | In-flight attempts retain global/group/resource reservations, including startup and cancellation confirmation; only a capacity-sized preparation window is claimed |
| Retry waiting | Wait for completion or the next retry deadline rather than repeatedly scanning control state every 50 milliseconds |
| Child state reads | Run and all dependency records come from one consistent snapshot instead of one read per dependency |
| Restart checks / summary | Historical attempted-node membership is loaded once; summaries derive outcomes and durations from one snapshot |
| Control history | Delta reads only events after the current sequence watermark; committed events are cached with continuity checks and refreshed for other writers |
| Source fingerprints | Versioned Delta inputs use table ID, pinned version, and schema; no full source-data hash is needed during planning or pre/post input checks |
| Output proofs | A child reuses its completed pinned-version proof for reads/final checkpoint; fresh reuse/explicit validation remains available |
| Spark transformations | Accepted/reject branches share a `MEMORY_AND_DISK` classification cache; it is unpersisted on both success and exception |
| Graph algorithms | Cached node index, adjacency-based topological sorting, and linear descendant traversal; a reverse-ordered 5,000-node chain visits each dependency list once |

The tests assert these behaviors directly rather than relying only on a noisy
wall-clock speed threshold. Native Delta tests additionally check that another
writer's committed suffix is observed, source recreation changes the identity
proof, modified references cannot use an old cached proof, and explicit output
validation still performs a fresh check.

## Platform-specific constraints

`runtime.scheduling` is `eager` for local/Databricks and `barrier` for Fabric.
Fabric continues to use one native `runMultiple` call for each ready wave in
the shared Spark session. Concurrent ad-hoc calls against that session are not
used as an unverified optimization. Parameterized notebook definitions can be
reused by multiple uniquely named DAG nodes.

ADLS Gen2 coordination uses a small, conditionally created lease file under the
configured directory. Infinite leases and ambiguous-write quarantine are
unchanged. Coordination is not removed to improve benchmark numbers.

Control events and immutable artifacts are Delta data in the Lakehouse.
Versioned sources, output retention, and append-only control history are
prerequisites for the optimizations. Do not modify underlying Delta files,
rewrite control events, or vacuum versions needed by active/recoverable runs.

For deployment sizing, measure actual source volumes and skew, Spark driver
memory, spill, notebook startup/queue time, control-transaction latency, ADLS
request rates, and end-to-end critical-path duration. Limits are per run: use
outer-job admission control when several business keys share a source/target
quota. The original DataStage workload and production capacities are still
unprovided, so no production SLA is asserted.
