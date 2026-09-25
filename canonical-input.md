# Canonical XML warehouse receiving example

This synthetic supply-chain workflow converts **advance shipment notices (ASNs)**
and product-master messages into an expected receiving plan. It does not claim
to implement an industry standard such as GS1/EPCIS, a specific WMS interface,
or an unprovided DataStage job. Map producer-specific XML into this explicit
canonical contract before adopting the example.

## Input set

```text
sample_data/
  canonical-v1.xsd
  expected_receiving_plan.json
  shipments/
    asn-1001.xml
    asn-1002.xml
    asn-2001.xml
    asn-exceptions.xml
  products/
    filters.xml
    seals.xml
```

Each configured source is a **directory of XML documents**, not one CSV or one
preloaded database table. Files with `.xml` extensions, case-insensitively, are
included recursively; unrelated files are ignored. Both cloud adapters use
`Files/canonical/shipments` and `Files/canonical/products` below the configured
Lakehouse root by default. Local mode reads the fixture directories above.

The producer must finish and freeze the complete file set before the business
run starts. The business run key is a date for the logical workload; it does not
infer a filter from filenames or automatically select today's documents.
Use a dedicated, immutable directory for each production snapshot.

## Canonical version 1.0

Both envelopes use namespace `urn:example:supply-chain:canonical:1` and exactly
`schemaVersion="1.0"`. Field order is defined by
[`canonical-v1.xsd`](sample_data/canonical-v1.xsd). The runtime enforces this
structure using the shared `spark_dag.canonical_xml` parser; it does not download
schemas or execute DTDs, external entities, or XInclude.

```xml
<ShipmentBatch xmlns="urn:example:supply-chain:canonical:1" schemaVersion="1.0">
  <ShipmentLine>
    <shipmentId>ASN-1001</shipmentId>
    <lineId>1</lineId>
    <sku>SKU-FILTER</sku>
    <warehouseId>WH-ATL</warehouseId>
    <quantity>80</quantity>
    <unitOfMeasure>EA</unitOfMeasure>
  </ShipmentLine>
</ShipmentBatch>
```

```xml
<ProductCatalog xmlns="urn:example:supply-chain:canonical:1" schemaVersion="1.0">
  <Product>
    <sku>SKU-FILTER</sku>
    <description>Replacement air filters</description>
    <unitOfMeasure>EA</unitOfMeasure>
  </Product>
</ProductCatalog>
```

An envelope can contain several records. Fields are strings at ingestion so an
invalid quantity can be quarantined as a **business reject** rather than silently
coerced or dropped. Empty field text becomes null. Identifiers and values are
case-sensitive; the parser does not silently trim or normalize producer values.

Unknown namespaces, envelope versions, fields, attributes, nested field content,
missing structural elements, malformed XML, and empty batches fail the **source
node**. No incomplete dataset is advertised as a successful checkpoint.
An empty *field* in an otherwise well-formed shipment is handled by the business
validation rules below.

## Receiving rules

| Rule | Behavior |
|---|---|
| Product master | SKU, description, and unit of measure must be nonempty; SKUs must be unique across the complete catalog input set |
| Shipment-line key | `(shipment_id, line_id)` must be present; different lines on the same ASN remain distinct |
| Exact retransmission | Collapse identical rows with the same compound key, including duplicates spread across files |
| Conflicting retransmission | Quarantine every distinct version of the key; do not choose an arbitrary winner |
| Quantity | Positive ASCII integer, at most 18 digits and no more than `data_quality.max_quantity`; zero, negative, whitespace-padded, fractional, or oversized values reject |
| Product reference | Unknown SKUs reject |
| Destination | Blank warehouse identifiers reject; this example has no separate warehouse master |
| Units | Shipment units must exactly match the product catalog; no implicit case-to-each conversion |
| Quality gate | Defaults permit at most two rejects and require at least three accepted lines |
| Plan | Group by warehouse, SKU, and unit of measure; sum exact signed-64-bit units and count accepted shipment lines |

The supplied file set contains six shipment rows and two product records.
`ASN-1002 / 1` is retransmitted, so it contributes 40 units once. `ASN-3001 / 1`
references an unknown SKU; `ASN-3002 / 1` has quantity `-5`. The final plan has
120 `SKU-FILTER` units for `WH-ATL` across two accepted lines and 60 `SKU-SEAL`
units for `WH-DFW` across one accepted line, all in `EA`.

The final outbox operation is `inventory_receipt`, with state `PENDING_DISPATCH`.
It references the committed receiving plan and a stable business-key/node/
operation identity. Identical recovery reuses the intent. Changed content under
that identity is an explicit conflict, not a duplicate stock posting. The
reference **does not dispatch the intent or modify WMS inventory**.

## Safety, restart proof, and capacity

- `sources.*.xml` declares the contract, canonical version, maximum bytes per
  file, maximum records per file, and maximum files. Defaults are 1 MiB per file,
  10,000 records per file, and 1,000 files per source.
- Local reads are byte-bounded. Spark reads the file set through `binaryFile`,
  checks file count/size metadata, and parses on executors with the same
  `defusedxml` implementation. Install the package and dependencies on every
  executor; no Spark-specific XML plugin is needed.
- XML membership, file names, and raw content hashes form the input fingerprint.
  Added, removed, renamed, or changed files invalidate prior success. Input
  proofs are checked around processing rather than trusting modification time.
  This requires XML byte reads; it is not the metadata-only optimization used
  for pinned Delta inputs.
- Parsing/size failures abort ingestion, while semantic line rejects are
  committed to the reject-data location. Shared Spark classification caching
  avoids recomputing joins for accepted and rejected outputs.
- Bound batches to the configured limits. For very large recurring XML feeds,
  use a separately governed canonical landing process and immutable Delta
  snapshots with matching canonical columns; the generic Delta source option
  remains supported. Measure actual XML parsing and hashing I/O before
  selecting production limits.

All samples are synthetic operational data, with no patient, customer, or
credential payloads.
