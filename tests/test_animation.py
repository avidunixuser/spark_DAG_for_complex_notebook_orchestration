from __future__ import annotations

import copy
import json
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from PIL import Image

from spark_dag.config_loader import load_config
from tools.dag_animation import (
    HEIGHT,
    MAIN_NODES,
    POSITIONS,
    STATE_COLORS,
    WIDTH,
    diagram_svg,
    render_html,
    sanitize_events,
    timeline,
)

ROOT = Path(__file__).resolve().parents[1]


class AnimationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recording = json.loads((ROOT / "docs" / "dag-recording.json").read_text(encoding="utf-8"))
        cls.config = load_config(deployment_dir=ROOT)

    def test_recording_matches_the_current_reference_dag(self):
        self.assertEqual(self.recording["code_fingerprint"], self.config.code_fingerprint)
        self.assertEqual(self.recording["configuration_fingerprint"], self.config.fingerprint)
        self.assertEqual({node["id"] for node in self.recording["nodes"]}, set(POSITIONS))
        for node in self.recording["nodes"]:
            self.assertEqual(node, {key: self.config.nodes[node["id"]][key] for key in node})
        for key in ("notify_failure", "notify_timeout"):
            self.assertEqual(set(self.config.nodes[key]["dependencies"]), set(MAIN_NODES))

    def test_recorded_scenarios_end_successfully_with_verified_output(self):
        self.assertEqual(
            {scenario["id"] for scenario in self.recording["scenarios"]},
            {"happy", "retry", "recovery", "timeout"},
        )
        for scenario in self.recording["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                self.assertEqual(scenario["final_status"], "SUCCEEDED")
                self.assertEqual(scenario["verified"]["delivery_intents"], 1)
                self.assertEqual(sum(row["total_cents"] for row in scenario["verified"]["report"]), 3550)
                frames = timeline(scenario, self.recording["nodes"])
                self.assertEqual(frames[-1]["run_status"], "SUCCEEDED")
                for frame in frames:
                    self.assertEqual(set(frame["states"]), set(POSITIONS))
                    self.assertTrue(
                        all(state["status"] in STATE_COLORS for state in frame["states"].values())
                    )
                    self.assertGreaterEqual(frame["duration_ms"], 100)

    def test_retry_and_recovery_are_distinct_recorded_paths(self):
        scenarios = {scenario["id"]: scenario for scenario in self.recording["scenarios"]}
        retry = scenarios["retry"]["events"]
        self.assertEqual({event["run"] for event in retry}, {1})
        self.assertTrue(any(event["event"] == "RETRY_SCHEDULED" and event["attempt"] == 2 for event in retry))
        for key in ("recovery", "timeout"):
            events = scenarios[key]["events"]
            self.assertEqual({event["run"] for event in events}, {1, 2})
            self.assertTrue(
                any(event["status"] == "SKIPPED_ALREADY_SATISFIED" and event["run"] == 2 for event in events)
            )
            self.assertTrue(
                any(event["event"] == "RUN_FINISHED" and event["status"] == "FAILED" for event in events)
            )
        self.assertTrue(any(event["status"] == "TIMED_OUT" for event in scenarios["timeout"]["events"]))

    def test_timeline_rejects_reordered_or_unknown_events(self):
        scenario = copy.deepcopy(self.recording["scenarios"][0])
        scenario["events"][1]["sequence"] = scenario["events"][0]["sequence"]
        with self.assertRaisesRegex(ValueError, "sequence"):
            timeline(scenario, self.recording["nodes"])
        scenario = copy.deepcopy(self.recording["scenarios"][0])
        scenario["events"][1]["node"] = "not_a_notebook"
        with self.assertRaisesRegex(ValueError, "unknown"):
            timeline(scenario, self.recording["nodes"])

    def test_public_events_are_allowlisted_not_raw_control_payloads(self):
        event = {
            "sequence": 1,
            "run_id": "private-run-id",
            "entity_type": "node",
            "event_type": "ATTEMPT_FINISHED",
            "payload": {
                "node_id": "quality_gate",
                "status": "FAILED",
                "restart_mode": "normal",
                "attempt": 1,
                "outputs": {"secret": "credential=private"},
                "parameters": "private-payload",
                "error": {"category": "DATA_QUALITY", "message": "private error"},
            },
        }
        sanitized = sanitize_events([event], {"private-run-id": 1})
        self.assertEqual(
            set(sanitized[0]),
            {"sequence", "run", "event", "node", "status", "mode", "attempt", "error_category"},
        )
        self.assertNotIn("private", json.dumps(sanitized))
        for scenario in self.recording["scenarios"]:
            for item in scenario["events"]:
                self.assertEqual(set(item), set(sanitized[0]))

    def test_svg_has_exactly_nine_notebook_nodes_and_accessible_description(self):
        root = ET.fromstring(diagram_svg(self.recording["nodes"]))
        namespace = {"svg": "http://www.w3.org/2000/svg"}
        ids = [element.attrib["id"] for element in root.findall("svg:g", namespace)]
        self.assertEqual(set(ids), {f"node-{key}" for key in POSITIONS})
        self.assertIsNotNone(root.find("svg:title", namespace))
        self.assertIsNotNone(root.find("svg:desc", namespace))

    def test_player_is_reproducible_self_contained_and_escapes_embedded_json(self):
        template = (ROOT / "tools" / "dag_player.html").read_text(encoding="utf-8")
        generated = render_html(self.recording, template)
        self.assertEqual(generated, (ROOT / "docs" / "dag-player.html").read_text(encoding="utf-8"))
        self.assertNotIn("<script src=", generated)
        self.assertNotIn("fetch(", generated)
        self.assertIn("prefers-reduced-motion: reduce", generated)
        malicious = copy.deepcopy(self.recording)
        malicious["provenance"] = "</script><script>alert(1)</script>"
        self.assertNotIn(malicious["provenance"], render_html(malicious, template))

    def test_readme_gif_is_animated_bounded_and_has_a_static_alternative(self):
        gif = ROOT / "docs" / "dag-animation.gif"
        self.assertLess(gif.stat().st_size, 1024 * 1024)
        with Image.open(gif) as image:
            self.assertEqual(image.size, (WIDTH, HEIGHT))
            self.assertGreater(image.n_frames, 20)
            self.assertEqual(image.info["loop"], 0)
            duration = 0
            first = image.convert("RGB").tobytes()
            for frame in range(image.n_frames):
                image.seek(frame)
                duration += image.info["duration"]
            self.assertNotEqual(first, image.convert("RGB").tobytes())
            self.assertGreater(duration, 10_000)
            self.assertLess(duration, 90_000)
        with Image.open(ROOT / "docs" / "dag-poster.png") as poster:
            self.assertEqual(poster.size, (WIDTH, HEIGHT))
