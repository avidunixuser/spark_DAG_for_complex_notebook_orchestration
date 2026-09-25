"""Record isolated sample runs and render offline DAG playback assets."""

from __future__ import annotations

import argparse
import copy
import html
import json
import math
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WIDTH, HEIGHT = 1320, 830
CARD_W, CARD_H = 218, 84
POSITIONS = {
    "extract_orders": (40, 212),
    "extract_customers": (40, 350),
    "transform_orders": (298, 282),
    "quality_gate": (548, 282),
    "publish_report": (798, 282),
    "deliver_report": (1048, 282),
    "notify_failure": (420, 545),
    "notify_timeout": (710, 545),
    "cleanup": (1048, 665),
}
STATE_COLORS = {
    "PENDING": "#64748b",
    "READY": "#38bdf8",
    "RUNNING": "#38bdf8",
    "SUCCEEDED": "#34d399",
    "FAILED": "#fb7185",
    "TIMED_OUT": "#fbbf24",
    "SKIPPED_ALREADY_SATISFIED": "#c4b5fd",
    "SKIPPED_CONDITION": "#94a3b8",
    "BLOCKED": "#fb923c",
    "CANCELLED": "#fb923c",
}
STATE_LABELS = {
    **{key: key.replace("_", " ").title() for key in STATE_COLORS},
    "SKIPPED_ALREADY_SATISFIED": "Reused checkpoint",
    "SKIPPED_CONDITION": "Condition skipped",
}
MAIN_NODES = tuple(POSITIONS)[:6]
SCENARIOS = (
    ("happy", "Happy path", "Two extracts run independently; the join waits for both.", None, False),
    (
        "retry",
        "Transient retry",
        "A transient transformation failure retries within the same run.",
        "retryable",
        False,
    ),
    (
        "recovery",
        "Failure and resume",
        "A failed transformation is repaired; valid extract checkpoints are reused.",
        "permanent",
        True,
    ),
    (
        "timeout",
        "Timeout and resume",
        "The local timed-out process is stopped before recovery.",
        "timeout",
        True,
    ),
)


def node_caption(node_id: str, status: str, attempt: int) -> str:
    label = STATE_LABELS[status]
    return f"{label}  |  attempt {attempt}" if attempt else label


def sanitize_events(events: list[dict[str, Any]], run_numbers: dict[str, int]) -> list[dict[str, Any]]:
    result = []
    for event in events:
        payload = event["payload"]
        if event["run_id"] not in run_numbers or event["entity_type"] not in {"run", "node"}:
            continue
        if event["event_type"] in {"NODE_CREATED", "REMOTE_RUN_SUBMITTED"}:
            continue
        result.append(
            {
                "sequence": event["sequence"],
                "run": run_numbers[event["run_id"]],
                "event": event["event_type"],
                "node": payload.get("node_id"),
                "status": payload["status"],
                "mode": payload["restart_mode"],
                "attempt": payload["attempt"],
                "error_category": (payload.get("error") or {}).get("category"),
            }
        )
    return result


def record(root: Path) -> dict[str, Any]:
    from spark_dag.artifacts import filesystem_path
    from spark_dag.config_loader import load_config
    from spark_dag.dag_validator import validate_dag
    from spark_dag.model import RunRequest, WorkflowError, fingerprint
    from spark_dag.orchestrator import Orchestrator

    original = load_config(deployment_dir=root)
    validate_dag(original)
    if (
        original.data["original_datastage_job"] != "NOT_SUPPLIED_REFERENCE_ONLY"
        or set(original.nodes) != set(POSITIONS)
        or original.data["runtime"]["platform"] != "local"
        or any(source["format"] != "csv" for source in original.data["sources"].values())
    ):
        raise WorkflowError("SAMPLE_ONLY", "The recorder only executes the bundled local reference DAG.")
    base = json.loads((root / "job_config.json").read_text(encoding="utf-8"))
    document = {
        "schema_version": 1,
        "dag_id": original.data["dag"]["id"],
        "dag_version": original.data["dag"]["version"],
        "code_fingerprint": original.code_fingerprint,
        "configuration_fingerprint": original.fingerprint,
        "provenance": "Recorded local reference runs with injected faults, not live cloud telemetry.",
        "timing": "Audit sequence order is preserved. Playback uses fixed presentation intervals, not wall-clock durations.",
        "nodes": [
            {key: node[key] for key in ("id", "description", "dependencies", "trigger")}
            for node in original.data["nodes"]
        ],
        "scenarios": [],
    }
    scratch = root / ".runtime"
    scratch.mkdir(exist_ok=True)
    for scenario_id, title, description, fault, resume in SCENARIOS:
        temporary = tempfile.TemporaryDirectory(
            prefix="dag-animation-", dir=scratch if os.name == "nt" else None
        )
        deployment = Path(temporary.name)
        try:
            for name in ("job_config.schema.json", "orchestrator.ipynb"):
                shutil.copy2(root / name, deployment / name)
            for name in ("notebooks", "sample_data"):
                shutil.copytree(root / name, deployment / name)
            data = copy.deepcopy(base)
            data.update(environment="local", environment_overrides={"local": {}}, force_rerun=False)
            data["force_restart"] = {"enabled": False, "from_node": None}
            data["logging"]["enabled"] = False
            data["control_store"] = {"backend": "sqlite", "path": ".runtime/control.sqlite3"}
            data["storage"] = {"backend": "local", "path": ".runtime/outputs"}
            data["reject_data"] = {"path": ".runtime/rejects"}
            data["sources"]["orders"]["path"] = "sample_data/orders.csv"
            data["sources"]["customers"]["path"] = "sample_data/customers.csv"
            data["runtime"].update(platform="local", allow_fault_injection=True, fault_injection={})
            node_id = "extract_orders" if fault == "timeout" else "transform_orders"
            if fault:
                data["runtime"]["fault_injection"][node_id] = {
                    "kind": fault,
                    "attempts": [1],
                    **({"delay_seconds": 15} if fault == "timeout" else {}),
                }
            if fault == "timeout":
                target = next(node for node in data["nodes"] if node["id"] == node_id)
                target["timeout_seconds"] = 2
                target["retry"]["max_retries"] = 0
            config_path = deployment / "job_config.json"
            config_path.write_text(json.dumps(data), encoding="utf-8")
            engine = Orchestrator(load_config(deployment_dir=deployment))
            first = engine.run(RunRequest("2026-09-24"))
            runs = [first]
            if resume:
                if first["status"] != "FAILED":
                    raise RuntimeError(f"{scenario_id}: expected the injected run to fail.")
                data["runtime"]["fault_injection"] = {}
                if fault == "timeout":
                    target["timeout_seconds"] = 30
                config_path.write_text(json.dumps(data), encoding="utf-8")
                engine = Orchestrator(load_config(deployment_dir=deployment))
                runs.append(
                    engine.run(RunRequest("2026-09-24", "resume", first["run_id"], reason="DEMO-RECOVERY"))
                )
            final = runs[-1]
            scope = fingerprint([data["application_id"], data["dag"]["id"], "2026-09-24"])
            report = engine.services.artifacts.read(final["nodes"]["publish_report"]["outputs"]["report"])
            expected = [
                {"region": "North", "total_cents": 1500, "order_count": 2},
                {"region": "South", "total_cents": 2050, "order_count": 1},
            ]
            outbox = engine.store.view(scope).all("outbox")
            if (
                final["status"] != "SUCCEEDED"
                or sorted(report, key=lambda row: row["region"]) != expected
                or len(outbox) != 1
            ):
                raise RuntimeError(f"{scenario_id}: recorded output did not satisfy the sample contract.")
            if resume and not any(
                node["status"] == "SKIPPED_ALREADY_SATISFIED" for node in final["nodes"].values()
            ):
                raise RuntimeError(f"{scenario_id}: recovery did not reuse valid checkpoints.")
            if scenario_id == "retry" and final["nodes"]["transform_orders"]["attempt"] != 2:
                raise RuntimeError("The retry recording must contain exactly two transformation attempts.")
            events = sanitize_events(
                engine.store.backend.events(scope),
                {run["run_id"]: index + 1 for index, run in enumerate(runs)},
            )
            document["scenarios"].append(
                {
                    "id": scenario_id,
                    "title": title,
                    "description": description,
                    "events": events,
                    "final_status": final["status"],
                    "verified": {"report": expected, "delivery_intents": len(outbox)},
                }
            )
        finally:
            shutil.rmtree(filesystem_path(deployment))
            temporary.cleanup()
    return document


def timeline(scenario: dict[str, Any], nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    states = {node["id"]: {"status": "PENDING", "attempt": 0} for node in nodes}
    frames = []
    last_sequence = 0
    for event in scenario["events"]:
        if event["sequence"] <= last_sequence:
            raise ValueError("Playback must preserve increasing control-event sequence numbers.")
        last_sequence = event["sequence"]
        if event["event"] == "RUN_STARTED":
            states = {node["id"]: {"status": "PENDING", "attempt": 0} for node in nodes}
            caption = (
                "Resume: validate existing checkpoints"
                if event["mode"] == "resume"
                else "Start: validate and claim ready nodes"
            )
        elif event["node"]:
            if event["node"] not in states or event["status"] not in STATE_COLORS:
                raise ValueError("The recording contains an unknown node or state.")
            states[event["node"]] = {"status": event["status"], "attempt": event["attempt"]}
            caption = f"{event['node']}  /  {node_caption(event['node'], event['status'], event['attempt'])}"
        elif event["event"] == "RUN_FINISHED":
            caption = (
                "Run failed. The next recorded run follows manual remediation."
                if event["status"] == "FAILED"
                else "Run succeeded. Output verified; exactly one delivery intent."
            )
        else:
            continue
        frames.append(
            {
                "states": copy.deepcopy(states),
                "caption": caption,
                "run": event["run"],
                "mode": event["mode"],
                "event": event["event"],
                "sequence": event["sequence"],
                "focus": event["node"],
                "run_status": event["status"] if event["node"] is None else "RUNNING",
                "duration_ms": 2200
                if event["event"] in {"RUN_STARTED", "RUN_FINISHED"}
                else (
                    1300 if event["status"] in {"FAILED", "TIMED_OUT", "SKIPPED_ALREADY_SATISFIED"} else 550
                ),
            }
        )
    if not frames:
        raise ValueError("The scenario contains no playable state transitions.")
    return frames


def edges(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for node in nodes:
        if node["id"] not in MAIN_NODES:
            continue
        x, y = POSITIONS[node["id"]]
        for dependency in node["dependencies"]:
            start_x, start_y = POSITIONS[dependency]
            middle_x = (start_x + CARD_W + x) / 2
            result.append(
                {
                    "source": dependency,
                    "target": node["id"],
                    "dashed": False,
                    "points": [
                        (start_x + CARD_W, start_y + CARD_H / 2),
                        (middle_x, start_y + CARD_H / 2),
                        (middle_x, y + CARD_H / 2),
                        (x, y + CARD_H / 2),
                    ],
                }
            )
    result.extend(
        [
            {
                "source": "barrier",
                "target": "notify_failure",
                "dashed": True,
                "points": [(660, 454), (660, 506), (529, 506), (529, 545)],
            },
            {
                "source": "barrier",
                "target": "notify_timeout",
                "dashed": True,
                "points": [(660, 454), (660, 506), (819, 506), (819, 545)],
            },
            {
                "source": "deliver_report",
                "target": "cleanup",
                "dashed": False,
                "points": [(1266, 324), (1290, 324), (1290, 707), (1266, 707)],
            },
            {
                "source": "notify_failure",
                "target": "cleanup",
                "dashed": False,
                "points": [(529, 629), (529, 710), (1048, 710)],
            },
            {
                "source": "notify_timeout",
                "target": "cleanup",
                "dashed": False,
                "points": [(819, 629), (819, 687), (1048, 687)],
            },
        ]
    )
    return result


def diagram_svg(nodes: list[dict[str, Any]]) -> str:
    parts = [
        f'<svg id="dag" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="dag-title dag-description" xmlns="http://www.w3.org/2000/svg">',
        '<title id="dag-title">Restartable notebook DAG playback</title>',
        '<desc id="dag-description">Nine notebook nodes. Parallel extracts feed transformation, quality checks, publication, and delivery intent. Failure and timeout handlers wait for the six-node main path; cleanup waits for delivery and both handlers.</desc>',
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke"/></marker></defs>',
        '<rect width="1320" height="830" rx="22" fill="#0b1220"/>',
        '<text x="40" y="49" fill="#e2e8f0" font-size="26" font-weight="700">Restartable Spark DAG</text>',
        '<text x="40" y="80" fill="#94a3b8" font-size="16">RECORDED LOCAL DEMO  /  not live telemetry</text>',
        '<text id="svg-phase" x="1280" y="49" text-anchor="end" fill="#c4b5fd" font-size="18"></text>',
        '<text id="svg-caption" x="40" y="127" fill="#e2e8f0" font-size="17"></text>',
        '<rect x="24" y="181" width="1272" height="273" rx="16" fill="#101c30" stroke="#27364e"/>',
        '<text x="40" y="198" fill="#94a3b8" font-size="12">SIX-NODE MAIN PATH / SUCCESS DEPENDENCIES</text>',
        '<text x="40" y="493" fill="#94a3b8" font-size="15">Handlers wait for all six main-path nodes to be terminal.</text>',
        '<text x="40" y="518" fill="#94a3b8" font-size="13">The shared barrier is not an extra notebook.</text>',
        '<text x="420" y="535" fill="#fb7185" font-size="14">ANY FAILED</text>',
        '<text x="710" y="535" fill="#fbbf24" font-size="14">ANY TIMED OUT</text>',
        '<text x="1048" y="652" fill="#94a3b8" font-size="14">ALL DONE / BOOKKEEPING</text>',
        '<text x="40" y="585" fill="#94a3b8" font-size="15">ADLS Gen2 file coordination</text>',
        '<text x="40" y="610" fill="#94a3b8" font-size="15">Lakehouse Delta control + outputs</text>',
        '<text x="40" y="640" fill="#64748b" font-size="13">Local recording uses SQLite + files.</text>',
        '<text x="40" y="680" fill="#94a3b8" font-size="13">Delivery creates an outbox intent, not a transfer.</text>',
    ]
    for index, edge in enumerate(edges(nodes)):
        points = " ".join(f"{x:g},{y:g}" for x, y in edge["points"])
        dashed = 'stroke-dasharray="5 6"' if edge["dashed"] else ""
        parts.append(
            f'<polyline id="edge-{index}" class="edge" points="{points}" fill="none" stroke="#475569" stroke-width="2" '
            f'{dashed} marker-end="url(#arrow)"/>'
        )
    for node in nodes:
        key = node["id"]
        x, y = POSITIONS[key]
        parts.extend(
            [
                f'<g id="node-{key}" class="node" data-status="PENDING">',
                f"<title>{html.escape(node['description'])}</title>",
                f'<rect class="card" x="{x}" y="{y}" width="{CARD_W}" height="{CARD_H}" rx="12" fill="#142137" stroke="#64748b" stroke-width="2"/>',
                f'<circle class="state-dot" cx="{x + 17}" cy="{y + 20}" r="4" fill="#64748b"/>',
                f'<text x="{x + 30}" y="{y + 26}" fill="#f1f5f9" font-size="16" font-family="ui-monospace,monospace">{key}</text>',
                f'<text class="state-label" x="{x + 15}" y="{y + 57}" fill="#94a3b8" font-size="13">Pending</text>',
                "</g>",
            ]
        )
    legend = [
        ("RUNNING", "Running"),
        ("SUCCEEDED", "Succeeded"),
        ("FAILED", "Failed"),
        ("TIMED_OUT", "Timed out"),
        ("SKIPPED_ALREADY_SATISFIED", "Reused"),
        ("SKIPPED_CONDITION", "Skipped"),
    ]
    for index, (status, label) in enumerate(legend):
        x = 44 + index * 208
        parts.append(
            f'<circle cx="{x}" cy="790" r="5" fill="{STATE_COLORS[status]}"/><text x="{x + 14}" y="795" fill="#cbd5e1" font-size="14">{label}</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def render_html(document: dict[str, Any], template: str) -> str:
    payload = {
        **document,
        "scenarios": [
            {**scenario, "frames": timeline(scenario, document["nodes"])}
            for scenario in document["scenarios"]
        ],
        "colors": STATE_COLORS,
        "labels": STATE_LABELS,
        "edges": edges(document["nodes"]),
    }
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).replace("<", "\\u003c")
    return template.replace("<!--DAG_SVG-->", diagram_svg(document["nodes"])).replace("/*DAG_DATA*/", encoded)


def draw_frame(document: dict[str, Any], scenario: dict[str, Any], frame: dict[str, Any], phase: float):
    from PIL import Image, ImageDraw, ImageFont

    image = Image.new("RGB", (WIDTH, HEIGHT), "#0b1220")
    draw = ImageDraw.Draw(image)

    def text(x: int, y: int, value: str, size: int = 16, color: str = "#cbd5e1"):
        draw.text((x, y), value, fill=color, font=ImageFont.load_default(size=size))

    text(40, 25, "Restartable Spark DAG", 29, "#e2e8f0")
    text(40, 65, "RECORDED LOCAL DEMO  /  not live telemetry", 16, "#94a3b8")
    text(810, 30, f"{scenario['title']}  |  run {frame['run']}  /  {frame['mode']}", 17, "#c4b5fd")
    text(40, 107, frame["caption"], 19, "#e2e8f0")
    draw.rounded_rectangle((24, 181, 1296, 454), radius=16, fill="#101c30", outline="#27364e")
    text(40, 183, "SIX-NODE MAIN PATH / SUCCESS DEPENDENCIES", 12, "#94a3b8")
    text(40, 478, "Handlers wait for all six main-path nodes to be terminal.", 15, "#94a3b8")
    text(40, 505, "The shared barrier is not an extra notebook.", 13, "#94a3b8")
    text(420, 517, "ANY FAILED", 14, "#fb7185")
    text(710, 517, "ANY TIMED OUT", 14, "#fbbf24")
    text(1048, 637, "ALL DONE / BOOKKEEPING", 14, "#94a3b8")
    text(40, 568, "ADLS Gen2 file coordination", 15, "#94a3b8")
    text(40, 593, "Lakehouse Delta control + outputs", 15, "#94a3b8")
    text(40, 625, "Local recording uses SQLite + files.", 13, "#64748b")
    text(40, 665, "Delivery creates an outbox intent, not a transfer.", 13, "#94a3b8")
    for edge in edges(document["nodes"]):
        points = edge["points"]
        state = frame["states"][edge["target"]]["status"]
        color = (
            STATE_COLORS[state]
            if state in {"RUNNING", "SUCCEEDED", "SKIPPED_ALREADY_SATISFIED"}
            else "#475569"
        )
        draw.line(points, fill=color, width=2)
        end_x, end_y = points[-1]
        dx, dy = end_x - points[-2][0], end_y - points[-2][1]
        angle = math.atan2(dy, dx)
        draw.polygon(
            [
                (end_x, end_y),
                (end_x - 10 * math.cos(angle - 0.4), end_y - 10 * math.sin(angle - 0.4)),
                (end_x - 10 * math.cos(angle + 0.4), end_y - 10 * math.sin(angle + 0.4)),
            ],
            fill=color,
        )
        if state == "RUNNING":
            segments = [math.dist(a, b) for a, b in zip(points[:-1], points[1:], strict=True)]
            distance = sum(segments) * phase
            for a, b, length in zip(points[:-1], points[1:], segments, strict=True):
                if distance <= length and length:
                    ratio = distance / length
                    x, y = a[0] + (b[0] - a[0]) * ratio, a[1] + (b[1] - a[1]) * ratio
                    draw.ellipse((x - 4, y - 4, x + 4, y + 4), fill=color)
                    break
                distance -= length
    for key, (x, y) in POSITIONS.items():
        state = frame["states"][key]
        color = STATE_COLORS[state["status"]]
        draw.rounded_rectangle(
            (x, y, x + CARD_W, y + CARD_H),
            radius=12,
            fill="#142137",
            outline=color,
            width=4 if key == frame["focus"] else 2,
        )
        draw.ellipse((x + 13, y + 16, x + 21, y + 24), fill=color)
        text(x + 30, y + 10, key, 17, "#f1f5f9")
        text(x + 15, y + 45, node_caption(key, state["status"], state["attempt"]), 13, color)
    for index, (status, label) in enumerate(
        [
            ("RUNNING", "Running"),
            ("SUCCEEDED", "Succeeded"),
            ("FAILED", "Failed"),
            ("TIMED_OUT", "Timed out"),
            ("SKIPPED_ALREADY_SATISFIED", "Reused"),
            ("SKIPPED_CONDITION", "Skipped"),
        ]
    ):
        x = 44 + index * 208
        draw.ellipse((x - 5, 785, x + 5, 795), fill=STATE_COLORS[status])
        text(x + 14, 780, label, 14)
    return image


def render_gif(document: dict[str, Any], output: Path) -> None:
    from PIL import Image

    scenario = next(item for item in document["scenarios"] if item["id"] == "recovery")
    frames, durations = [], []
    # A fixed palette keeps colors stable across frames and avoids per-frame quantization noise.
    colors = list(
        dict.fromkeys(
            [
                "#0b1220",
                "#101c30",
                "#142137",
                "#27364e",
                "#475569",
                "#e2e8f0",
                "#f1f5f9",
                "#cbd5e1",
                *STATE_COLORS.values(),
            ]
        )
    )
    palette = Image.new("P", (1, 1))
    rgb = [int(color[index : index + 2], 16) for color in colors for index in (1, 3, 5)]
    palette.putpalette(rgb + [0] * (768 - len(rgb)))
    for frame in timeline(scenario, document["nodes"]):
        phases = (
            (0.25, 0.75)
            if any(state["status"] == "RUNNING" for state in frame["states"].values())
            else (0.5,)
        )
        for phase in phases:
            frames.append(
                draw_frame(document, scenario, frame, phase).quantize(
                    palette=palette, dither=Image.Dither.NONE
                )
            )
            durations.append(max(100, frame["duration_ms"] // len(phases)))
    frames[0].save(
        output / "dag-animation.gif",
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        disposal=1,
        optimize=True,
    )
    happy = next(item for item in document["scenarios"] if item["id"] == "happy")
    draw_frame(document, happy, timeline(happy, document["nodes"])[-1], 0.5).save(
        output / "dag-poster.png", optimize=True
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--record", action="store_true", help="Execute isolated local sample runs before rendering."
    )
    args = parser.parse_args(argv)
    sys.path.insert(0, str(ROOT))
    output = ROOT / "docs"
    output.mkdir(exist_ok=True)
    recording = output / "dag-recording.json"
    if args.record:
        recording.write_text(json.dumps(record(ROOT), indent=2) + "\n", encoding="utf-8")
    document = json.loads(recording.read_text(encoding="utf-8"))
    template = (ROOT / "tools" / "dag_player.html").read_text(encoding="utf-8")
    (output / "dag-player.html").write_text(render_html(document, template), encoding="utf-8")
    render_gif(document, output)
    print(
        json.dumps(
            {
                "scenarios": len(document["scenarios"]),
                "gif_bytes": (output / "dag-animation.gif").stat().st_size,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
