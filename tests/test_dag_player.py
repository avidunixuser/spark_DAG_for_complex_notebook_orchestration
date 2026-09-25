from __future__ import annotations

import os
import unittest
from pathlib import Path


@unittest.skipUnless(
    os.environ.get("SPARK_DAG_BROWSER_TESTS") == "1",
    "Enable SPARK_DAG_BROWSER_TESTS for browser playback checks.",
)
class DagPlayerBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright

        cls.playwright = sync_playwright().start()
        channel = os.environ.get("SPARK_DAG_BROWSER_CHANNEL")
        cls.browser = cls.playwright.chromium.launch(**({"channel": channel} if channel else {}))

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.context = self.browser.new_context()
        self.addCleanup(self.context.close)
        self.page = self.context.new_page()
        self.errors = []
        self.page.on("pageerror", lambda error: self.errors.append(str(error)))
        self.url = (Path(__file__).resolve().parents[1] / "docs" / "dag-player.html").as_uri()
        self.page.goto(self.url)
        self.page.wait_for_selector("#node-table tr", state="attached")

    def tearDown(self):
        self.assertEqual(self.errors, [])

    def seek_end(self):
        self.page.locator("#scrubber").evaluate(
            "(element) => { element.value = element.max; element.dispatchEvent(new Event('input')); }"
        )

    def test_pause_step_speed_scrub_and_scenario_switch(self):
        from playwright.sync_api import expect

        expect(self.page.locator("#play")).to_have_text("Play")
        expect(self.page.locator("#previous")).to_be_disabled()
        self.page.locator("#next").click()
        expect(self.page.locator("#scrubber")).to_have_value("1")
        self.page.locator("#previous").click()
        expect(self.page.locator("#scrubber")).to_have_value("0")
        self.page.locator("#speed").select_option("4")
        self.page.locator("#play").click()
        expect(self.page.locator("#play")).to_have_text("Pause")
        expect(self.page.locator("#scrubber")).not_to_have_value("0")
        self.page.locator("#play").click()
        paused = self.page.locator("#scrubber").input_value()
        self.page.wait_for_timeout(700)
        self.assertEqual(self.page.locator("#scrubber").input_value(), paused)
        for scenario in ("happy", "retry", "timeout", "recovery"):
            self.page.locator("#scenario").select_option(scenario)
            expect(self.page.locator("#scenario")).to_have_value(scenario)
            expect(self.page.locator("#scrubber")).to_have_value("0")
            self.seek_end()
            expect(self.page.locator("#next")).to_be_disabled()
            expect(self.page.locator("#svg-caption")).to_contain_text("Run succeeded")
        expect(self.page.locator("#node-extract_customers")).to_have_attribute(
            "data-status", "SKIPPED_ALREADY_SATISFIED"
        )
        self.page.locator("#restart").click()
        expect(self.page.locator("#scrubber")).to_have_value("0")
        expect(self.page.locator("#play")).to_have_text("Play")

    def test_reduced_motion_and_offline_small_screen(self):
        from playwright.sync_api import expect

        self.page.emulate_media(reduced_motion="reduce")
        self.page.set_viewport_size({"width": 390, "height": 844})
        network = []
        self.page.on("request", lambda request: network.append(request.url))
        self.page.locator("#speed").select_option("4")
        self.page.locator("#play").click()
        self.page.locator(".edge.flowing").first.wait_for(state="attached")
        animation = self.page.locator(".edge.flowing").first.evaluate(
            "(element) => getComputedStyle(element).animationName"
        )
        self.assertEqual(animation, "none")
        self.assertFalse(any(url.startswith(("http:", "https:")) for url in network))
        self.page.locator("#play").click()
        expect(self.page.locator("#error")).to_be_hidden()
        expect(self.page.locator("#node-table tr")).to_have_count(9)

    def test_auto_stop_at_end_and_replay(self):
        from playwright.sync_api import expect

        self.seek_end()
        expect(self.page.locator("#play")).to_have_text("Replay")
        self.page.locator("#play").click()
        expect(self.page.locator("#scrubber")).to_have_value("0")
        self.page.locator("#play").click()
        self.page.locator("#scrubber").evaluate(
            "(element) => { element.value = Number(element.max) - 1; element.dispatchEvent(new Event('input')); }"
        )
        self.page.locator("#speed").select_option("4")
        self.page.locator("#play").click()
        expect(self.page.locator("#play")).to_have_text("Replay", timeout=5000)
        expect(self.page.locator("#play")).to_have_attribute("aria-pressed", "false")

    def test_keyboard_navigation_and_hidden_tab_pause(self):
        from playwright.sync_api import expect

        self.page.locator(".diagram-scroll").focus()
        self.page.keyboard.press("ArrowRight")
        expect(self.page.locator("#scrubber")).to_have_value("1")
        self.page.keyboard.press("ArrowLeft")
        expect(self.page.locator("#scrubber")).to_have_value("0")
        self.page.keyboard.press("Space")
        expect(self.page.locator("#play")).to_have_text("Pause")
        self.page.evaluate(
            "() => { Object.defineProperty(document, 'hidden', {value: true, configurable: true}); "
            "document.dispatchEvent(new Event('visibilitychange')); }"
        )
        expect(self.page.locator("#play")).to_have_text("Play")
        expect(self.page.locator("#play")).to_have_attribute("aria-pressed", "false")
