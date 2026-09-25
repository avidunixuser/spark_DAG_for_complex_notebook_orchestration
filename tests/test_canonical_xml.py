from __future__ import annotations

import copy
from xml.etree import ElementTree

from spark_dag.canonical_xml import CONTRACTS, NAMESPACE, parse_document, schema_for
from spark_dag.components import transform_python
from spark_dag.model import Category, TaskFailure, WorkflowError
from tests.support import DeploymentTest, product, shipment, write_xml


class CanonicalXMLTests(DeploymentTest):
    def test_published_xsd_and_runtime_field_contracts_match(self):
        schema = ElementTree.parse(self.root / "sample_data" / "canonical-v1.xsd").getroot()
        namespace = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.assertEqual(schema.attrib["targetNamespace"], NAMESPACE)
        for definition in CONTRACTS.values():
            record_type = schema.find(f"xs:complexType[@name='{definition['record']}Type']", namespace)
            self.assertIsNotNone(record_type)
            self.assertEqual(
                [
                    element.attrib["name"]
                    for element in record_type.findall("xs:sequence/xs:element", namespace)
                ],
                [name for name, _ in definition["fields"]],
            )
            root = schema.find(f"xs:element[@name='{definition['root']}']", namespace)
            self.assertEqual(root.find("xs:complexType/xs:attribute", namespace).attrib["fixed"], "1.0")

    def test_multiple_xml_files_are_combined_and_retransmissions_deduplicated(self):
        engine = self.engine()
        shipments = engine.services.artifacts.read_source(self.data["sources"]["shipments"])
        products = engine.services.artifacts.read_source(self.data["sources"]["products"])
        self.assertEqual(len(list((self.root / "sample_data").rglob("*.xml"))), 6)
        self.assertEqual(len(shipments), 6)
        self.assertEqual(len(products), 2)
        accepted, rejected = transform_python(shipments, products, 1000000)
        self.assertEqual(len(accepted), 3)
        self.assertEqual({row["reason"] for row in rejected}, {"unknown_sku", "invalid_quantity"})
        self.assertEqual(sum(row["quantity"] for row in accepted), 180)

    def test_fingerprint_covers_membership_names_and_file_contents(self):
        engine = self.engine()
        source = self.data["sources"]["shipments"]
        initial = engine.services.artifacts.source_fingerprint(source)
        extra = self.add_shipment()
        added = engine.services.artifacts.source_fingerprint(source)
        self.assertNotEqual(initial, added)
        extra.rename(extra.with_name("renamed.xml"))
        renamed = engine.services.artifacts.source_fingerprint(source)
        self.assertNotEqual(added, renamed)
        extra = extra.with_name("renamed.xml")
        extra.write_bytes(extra.read_bytes().replace(b">10<", b">11<"))
        self.assertNotEqual(renamed, engine.services.artifacts.source_fingerprint(source))
        extra.unlink()
        self.assertEqual(initial, engine.services.artifacts.source_fingerprint(source))

    def test_xml_files_in_subdirectories_and_uppercase_extension_are_included(self):
        nested = self.root / "sample_data" / "shipments" / "supplier"
        nested.mkdir()
        write_xml(nested / "additional.XML", "shipment_batch", [shipment(shipment_id="ASN-NESTED")])
        rows = self.engine().services.artifacts.read_source(self.data["sources"]["shipments"])
        self.assertEqual(len(rows), 7)
        self.assertIn("ASN-NESTED", {row["shipment_id"] for row in rows})

    def test_unsupported_xml_version_namespace_and_unknown_fields_fail_closed(self):
        content = (self.root / "sample_data" / "shipments" / "asn-1001.xml").read_bytes()
        settings = self.data["sources"]["shipments"]["xml"]
        cases = [
            content.replace(b'schemaVersion="1.0"', b'schemaVersion="2.0"'),
            content.replace(NAMESPACE.encode(), b"urn:wrong:namespace"),
            content.replace(b"</ShipmentLine>", b"<unknown>value</unknown></ShipmentLine>"),
            content.replace(b"<lineId>1</lineId>", b""),
            content.replace(b"<quantity>80</quantity>", b"<quantity><value>80</value></quantity>"),
            content.replace(b"<quantity>80</quantity>", b'<quantity unit="EA">80</quantity>'),
        ]
        for document in cases:
            with self.subTest(xml=document[:70]), self.assertRaises(TaskFailure) as error:
                parse_document(document, settings)
            self.assertEqual(error.exception.category, Category.DATA_QUALITY)

    def test_dtd_and_external_entities_are_forbidden_without_echoing_content(self):
        content = (self.root / "sample_data" / "shipments" / "asn-1001.xml").read_bytes()
        injection = b'<!DOCTYPE ShipmentBatch [<!ENTITY demo SYSTEM "file:///do-not-access">]>'
        content = content.replace(b"<ShipmentBatch", injection + b"<ShipmentBatch", 1)
        with self.assertRaises(TaskFailure) as error:
            parse_document(content, self.data["sources"]["shipments"]["xml"])
        self.assertEqual(error.exception.code, "UNSAFE_XML")
        self.assertNotIn("do-not-access", str(error.exception))

    def test_malformed_and_empty_batches_are_clear_failures(self):
        settings = self.data["sources"]["shipments"]["xml"]
        cases = [
            (b"<broken", "MALFORMED_XML"),
            (f'<ShipmentBatch xmlns="{NAMESPACE}" schemaVersion="1.0"/>'.encode(), "EMPTY_XML_BATCH"),
        ]
        for content, code in cases:
            with self.subTest(code=code), self.assertRaises(TaskFailure) as error:
                parse_document(content, settings)
            self.assertEqual(error.exception.code, code)

    def test_declared_file_record_and_file_count_limits_are_enforced(self):
        source = copy.deepcopy(self.data["sources"]["shipments"])
        content = (self.root / "sample_data" / "shipments" / "asn-exceptions.xml").read_bytes()
        with self.assertRaises(TaskFailure) as records:
            parse_document(content, {**source["xml"], "max_records_per_file": 1})
        self.assertEqual(records.exception.code, "XML_RECORD_LIMIT")
        source["xml"]["max_file_bytes"] = 10
        with self.assertRaises(TaskFailure) as size:
            self.engine().services.artifacts.read_source(source)
        self.assertEqual(size.exception.code, "XML_FILE_TOO_LARGE")
        source = copy.deepcopy(self.data["sources"]["shipments"])
        source["xml"]["max_files"] = 1
        with self.assertRaises(TaskFailure) as files:
            self.engine().services.artifacts.source_fingerprint(source)
        self.assertEqual(files.exception.code, "XML_FILE_LIMIT")

    def test_business_rejects_do_not_abort_well_formed_xml_extraction(self):
        rows = [
            shipment(quantity="-1"),
            shipment(shipment_id="ASN-ZERO", quantity="0"),
            shipment(shipment_id="ASN-UNIT", unit_of_measure="CASE"),
            shipment(shipment_id="ASN-NO-WH", warehouse_id=None),
            shipment(shipment_id="ASN-NO-LINE", line_id=None),
        ]
        path = self.root / "sample_data" / "shipments" / "additional.xml"
        write_xml(path, "shipment_batch", rows)
        parsed = parse_document(path.read_bytes(), self.data["sources"]["shipments"]["xml"])
        accepted, rejected = transform_python(parsed, [product()], 1000)
        self.assertEqual(accepted, [])
        self.assertEqual(
            {row["reason"] for row in rejected},
            {"invalid_quantity", "unit_mismatch", "missing_warehouse", "missing_shipment_key"},
        )

    def test_shipment_line_compound_key_is_not_just_shipment_id(self):
        accepted, rejected = transform_python(
            [shipment(line_id="1"), shipment(line_id="2")], [product()], 1000
        )
        self.assertEqual(len(accepted), 2)
        self.assertEqual(rejected, [])
        accepted, rejected = transform_python(
            [shipment(quantity="1"), shipment(quantity="2")], [product()], 1000
        )
        self.assertEqual(accepted, [])
        self.assertEqual({row["reason"] for row in rejected}, {"conflicting_shipment_key"})

    def test_duplicate_or_incomplete_product_catalog_fails(self):
        for rows in ([product(), product()], [product(unit_of_measure=None)], [product(description=" ")]):
            with self.subTest(products=rows), self.assertRaises(TaskFailure) as error:
                transform_python([shipment()], rows, 1000)
            self.assertEqual(error.exception.code, "INVALID_DIMENSION")

    def test_config_schema_matches_the_declared_xml_contract(self):
        for contract in ("shipment_batch", "product_catalog"):
            self.assertIn("STRING", schema_for(contract))
        self.data["sources"]["shipments"]["schema"] = "value STRING"
        with self.assertRaisesRegex(WorkflowError, "canonical XML contract"):
            self.config()

    def test_malformed_file_failure_then_resume_preserves_unrelated_product_branch(self):
        path = self.root / "sample_data" / "shipments" / "asn-1001.xml"
        original = path.read_bytes()
        path.write_bytes(b"<ShipmentBatch")
        first = self.run_dag()
        self.assertEqual(first["status"], "FAILED")
        self.assertEqual(first["nodes"]["extract_shipments"]["error"]["code"], "MALFORMED_XML")
        self.assertEqual(first["nodes"]["extract_shipments"]["attempt"], 1)
        path.write_bytes(original)
        resumed = self.run_dag("resume", first["run_id"])
        self.assert_receiving_plan(resumed)
        self.assertEqual(resumed["nodes"]["extract_products"]["status"], "SKIPPED_ALREADY_SATISFIED")

    def test_non_xml_files_do_not_change_the_input_fingerprint(self):
        engine = self.engine()
        source = self.data["sources"]["shipments"]
        initial = engine.services.artifacts.source_fingerprint(source)
        (self.root / "sample_data" / "shipments" / "README.txt").write_text("Landing metadata")
        self.assertEqual(initial, engine.services.artifacts.source_fingerprint(source))
