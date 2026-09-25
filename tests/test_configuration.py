from __future__ import annotations

import copy
import json
from pathlib import Path

import nbformat

from spark_dag.config_loader import load_config, resolve_deployment
from spark_dag.dag_validator import validate_dag
from spark_dag.model import WorkflowError
from tests.support import ROOT, DeploymentTest


class ConfigurationTests(DeploymentTest):
    def test_valid_json_configuration(self):
        config = self.config()
        self.assertEqual(config.data["schema_version"], "1.2")
        self.assertEqual(len(config.fingerprint), 64)
        self.assertEqual(len(validate_dag(config)), 9)

    def test_unsupported_configuration_version(self):
        self.data["schema_version"] = "2.0"
        with self.assertRaisesRegex(WorkflowError, "schema version"):
            self.config()

    def test_missing_sidecar(self):
        with self.assertRaisesRegex(WorkflowError, "does not exist"):
            load_config("missing.json", deployment_dir=self.root)

    def test_malformed_json(self):
        (self.root / "job_config.json").write_text('{"secret":"do-not-echo",', encoding="utf-8")
        with self.assertRaises(WorkflowError) as result:
            load_config(deployment_dir=self.root)
        self.assertEqual(result.exception.code, "MALFORMED_JSON")
        self.assertNotIn("do-not-echo", str(result.exception))

    def test_missing_required_configuration(self):
        for field in ("sources", "retry", "configuration_version", "control_store"):
            with self.subTest(field=field):
                if field == "retry":
                    value = self.data["nodes"][0].pop("retry")
                else:
                    value = self.data.pop(field)
                with self.assertRaises(WorkflowError):
                    self.config()
                if field == "retry":
                    self.data["nodes"][0][field] = value
                else:
                    self.data[field] = value

    def test_duplicate_json_properties_rejected(self):
        (self.root / "job_config.json").write_text('{"schema_version":"1.0","schema_version":"1.0"}')
        with self.assertRaisesRegex(WorkflowError, "Duplicate"):
            load_config(deployment_dir=self.root)

    def test_nonfinite_json_numbers_rejected(self):
        (self.root / "job_config.json").write_text('{"schema_version":"1.0","value":NaN}')
        with self.assertRaisesRegex(WorkflowError, "Non-finite"):
            load_config(deployment_dir=self.root)

    def test_sidecar_filename_override(self):
        (self.root / "alternate.json").write_text(json.dumps(self.data), encoding="utf-8")
        config = load_config("alternate.json", deployment_dir=self.root)
        self.assertEqual(config.path.name, "alternate.json")
        self.assertEqual(load_config(str(config.path)).fingerprint, config.fingerprint)

    def test_environment_overrides_are_deep_and_explicit(self):
        self.data["environment_overrides"]["test"] = {"data_quality": {"max_reject_count": 0}}
        config = self.config(environment="test")
        self.assertEqual(config.data["data_quality"]["max_reject_count"], 0)
        self.assertEqual(config.data["data_quality"]["minimum_accepted_rows"], 3)
        with self.assertRaisesRegex(WorkflowError, "no explicit override"):
            self.config(environment="unknown")

    def test_schema_override_rejected(self):
        self.data["environment_overrides"]["local"]["schema_version"] = "2.0"
        with self.assertRaisesRegex(WorkflowError, "schema metadata"):
            self.config()

    def test_inline_secrets_and_uri_credentials_rejected(self):
        for value in (
            {"password": "never-log-this"},
            {"endpoint": "https://user:never-log-this@example.invalid"},
            {"endpoint": "https://example.invalid?sig=never-log-this"},
        ):
            with self.subTest(shape=list(value)):
                self.data["targets"]["extra"] = value
                with self.assertRaises(WorkflowError) as result:
                    self.config()
                self.assertNotIn("never-log-this", str(result.exception))

    def test_cloud_placeholders_fail_closed(self):
        for environment in ("fabric", "databricks"):
            with self.subTest(platform=environment):
                with self.assertRaisesRegex(WorkflowError, "placeholders"):
                    self.config(environment=environment)

    def configured_cloud(self, platform="fabric"):
        overrides = self.data["environment_overrides"][platform]
        overrides["lakehouse"]["root_uri"] = (
            "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse-id"
        )
        overrides["locking"]["account_url"] = "https://coordination.dfs.core.windows.net"
        if platform == "databricks":
            overrides["notebook_base_path"] = "/Shared/example/notebooks"
            overrides["runtime"]["databricks"]["existing_cluster_id"] = "cluster-id"
        return overrides

    def test_lakehouse_paths_resolve_for_both_cloud_adapters(self):
        for platform in ("fabric", "databricks"):
            with self.subTest(platform=platform):
                self.configured_cloud(platform)
                config = self.config(environment=platform)
                root = config.data["lakehouse"]["root_uri"]
                self.assertEqual(config.data["control_store"]["path"], root + "/Tables/dag_control_events")
                self.assertEqual(
                    config.data["sources"]["shipments"]["path"], root + "/Files/canonical/shipments"
                )
                self.assertEqual(config.data["storage"]["path"], root + "/Files/dag/artifacts")

    def test_lakehouse_paths_cannot_escape_managed_folders(self):
        overrides = self.configured_cloud()
        overrides["storage"]["path"] = "Files/../../outside"
        with self.assertRaisesRegex(WorkflowError, "inside Tables or Files"):
            self.config(environment="fabric")

    def test_lakehouse_nonstandard_port_is_not_silently_discarded(self):
        overrides = self.configured_cloud()
        overrides["lakehouse"]["root_uri"] = (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com:8443/lakehouse"
        )
        with self.assertRaisesRegex(WorkflowError, "canonical"):
            self.config(environment="fabric")

    def test_fabric_eager_scheduling_is_not_allowed(self):
        overrides = self.configured_cloud()
        overrides["runtime"]["scheduling"] = "eager"
        with self.assertRaisesRegex(WorkflowError, "native runMultiple"):
            self.config(environment="fabric")

    def test_fabric_fanout_can_reuse_a_parameterized_notebook(self):
        self.configured_cloud()
        node = copy.deepcopy(self.node("cleanup"))
        node.update(id="second_cleanup", dependencies=[])
        node["checkpoint"]["name"] = "second_cleanup_done"
        self.data["nodes"].append(node)
        self.data["dag"]["entry_nodes"].append("second_cleanup")
        self.assertIn("second_cleanup", validate_dag(self.config(environment="fabric")))

    def test_old_blob_schema_is_not_silently_accepted(self):
        self.data["schema_version"] = "1.0"
        with self.assertRaisesRegex(WorkflowError, "1.2"):
            self.config()

    def test_cloud_sqlite_is_rejected(self):
        self.data["runtime"]["platform"] = "fabric"
        with self.assertRaisesRegex(WorkflowError, "Cloud runs require"):
            self.config()

    def test_abfss_container_authority_is_not_mistaken_for_credentials(self):
        self.data["targets"]["extra"] = {"path": "abfss://data@account.dfs.core.windows.net/receiving_plan"}
        self.config()

    def test_spark_hadoop_account_keys_are_rejected(self):
        self.data["spark"]["settings"]["spark.hadoop.fs.azure.account.key.account.dfs.core.windows.net"] = (
            "secret"
        )
        with self.assertRaisesRegex(WorkflowError, "credential"):
            self.config()

    def test_missing_secret_reference_is_rejected(self):
        self.data["notifications"]["secret_ref"] = "undefined"
        with self.assertRaisesRegex(WorkflowError, "undefined secret"):
            self.config()

    def test_secret_reference_names_may_contain_token(self):
        self.data["secret_references"]["api_token"] = {
            "provider": "azure_key_vault",
            "name": "delivery-token",
            "vault_url": "https://example.vault.azure.net",
        }
        self.data["notifications"]["secret_ref"] = "api_token"
        self.config()

    def test_fault_injection_requires_opt_in(self):
        self.data["runtime"]["allow_fault_injection"] = False
        self.fault("extract_shipments")
        with self.assertRaisesRegex(WorkflowError, "explicitly enabled"):
            self.config()

    def test_force_flags_are_distinct_and_mutually_exclusive(self):
        self.data["force_restart"] = {"enabled": True, "from_node": "quality_gate"}
        self.data["force_rerun"] = True
        with self.assertRaisesRegex(WorkflowError, "mutually exclusive"):
            self.config()

    def test_sidecar_must_resolve_in_deployment_folder(self):
        with self.assertRaises(WorkflowError):
            resolve_deployment(str(self.root / "job_config.json"), ROOT)
        with self.assertRaises(WorkflowError):
            resolve_deployment(str(Path("nested") / "job_config.json"), self.root)

    def test_notebooks_validate_and_compile(self):
        paths = list(self.root.glob("*.ipynb")) + list((self.root / "notebooks").glob("*.ipynb"))
        self.assertEqual(len(paths), 11)
        for path in paths:
            with self.subTest(notebook=path.name):
                notebook = nbformat.read(path, as_version=4)
                nbformat.validate(notebook)
                self.assertTrue(any("parameters" in cell.metadata.get("tags", []) for cell in notebook.cells))
                for cell in notebook.cells:
                    if cell.cell_type == "code":
                        compile(cell.source, str(path), "exec")
