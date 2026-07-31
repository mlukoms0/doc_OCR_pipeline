-- =====================================================================
-- BigQuery setup for the freight extraction project.
-- Project is set to 144240581301 below.
--   NOTE: BigQuery SQL and bq want the project *ID* (e.g. "linic-freight-123"),
--   not the numeric project *number*. If a query errors with
--   "Not found: Project 144240581301", Find & Replace it with your project ID.
-- The dataset is called `freight` and lives in the US multi-region.
--
-- *** THIS FILE IS GENERATED-SHAPED. ***
-- The staging struct, the `documents` DDL and both halves of the INSERT are all
-- derived from the Pydantic Document model in pipeline/extract.py. If you add a
-- field there, regenerate rather than hand-editing four places and hoping they line
-- up — that is exactly how a positional INSERT silently writes currency into
-- rate_is_all_in. 66 document fields, 72 inserted columns.
--
-- Order of operations:
--   1) Create the dataset (command line):
--        bq --location=US mk -d 144240581301:freight
--   2) Run section A below to create the tables.
--   3) After extract.py has written JSON to gs://<doc-archive67>/json/, load it
--      (command line):
--        bq load --replace --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values \
--          144240581301:freight.stg_extractions "gs://doc-archive67/json/*.json"
--
--      *** --replace IS NOT OPTIONAL ***
--      bq load APPENDS by default. extract.py is resumable, so you WILL load more
--      than once — and every extra load silently re-counted every packet already in
--      staging, doubling rows all the way through to gold. --replace rebuilds staging
--      from the whole json/ prefix each time, which is always what you want: the GCS
--      JSON is the lossless source of truth and re-reading all of it costs nothing.
--   4) Run section B to flatten into the final tables.
--   5) Then run gold_layer.sql (it reads freight.documents).
--
-- Section A uses CREATE OR REPLACE on purpose. Both tables are derived — staging is
-- rebuilt from GCS, `documents` is rebuilt from staging — so a schema change here has
-- to actually take effect. Under CREATE TABLE IF NOT EXISTS, a newly added field was
-- silently ignored wherever the table already existed. Run section A BEFORE the load.
-- =====================================================================


-- ========== SECTION A: create tables =================================

-- Staging: mirrors exactly what extract.py writes (one row per PDF). Dates and money
-- stay STRING/FLOAT here and are cleaned in Section B. Fields INSIDE the repeated
-- structs (stops/accessorials/references) keep these types in BOTH tables — the INSERT
-- copies those arrays wholesale, so the struct definitions must match exactly.
CREATE OR REPLACE TABLE `144240581301.freight.stg_extractions` (
  load_id STRING,
  source_pdf STRING,
  packet_summary STRING,
  documents ARRAY<STRUCT<
    doc_type STRING,
    page_range STRING,
    illegible BOOL,
    is_signed_delivery_copy BOOL,
    delivery_signed_date STRING,
    load_number STRING,
    pro_number STRING,
    bol_number STRING,
    order_number STRING,
    rc_number STRING,
    broker_name STRING,
    broker_mc STRING,
    carrier_name STRING,
    carrier_mc STRING,
    carrier_dot STRING,
    carrier_scac STRING,
    shipper_name STRING,
    shipper_address STRING,
    consignee_name STRING,
    consignee_address STRING,
    origin_city STRING,
    origin_state STRING,
    origin_zip STRING,
    destination_city STRING,
    destination_state STRING,
    destination_zip STRING,
    pickup_date STRING,
    delivery_date STRING,
    pickup_appt_start STRING,
    pickup_appt_end STRING,
    delivery_appt_start STRING,
    delivery_appt_end STRING,
    rate_total FLOAT64,
    line_haul FLOAT64,
    fuel_surcharge FLOAT64,
    rate_is_all_in BOOL,
    currency STRING,
    miles FLOAT64,
    payment_net_days INT64,
    quick_pay_pct FLOAT64,
    commodity STRING,
    weight FLOAT64,
    weight_unit STRING,
    pieces INT64,
    pallet_count INT64,
    pallet_spaces INT64,
    equipment_class STRING,
    equipment_length_ft INT64,
    is_team BOOL,
    equipment_type_raw STRING,
    temperature_setpoint_f FLOAT64,
    temperature_mode STRING,
    freight_class STRING,
    hazmat BOOL,
    seal_number STRING,
    delivered BOOL,
    received_by STRING,
    signature_present BOOL,
    exceptions_notes STRING,
    pieces_received INT64,
    osd_code STRING,
    stops ARRAY<STRUCT<sequence INT64, stop_type STRING, name STRING, address STRING, city STRING, state STRING, zip STRING, scheduled_date STRING, appt_start STRING, appt_end STRING, appt_type STRING, reference_number STRING>>,
    accessorials ARRAY<STRUCT<type STRING, amount FLOAT64, notes STRING>>,
    references ARRAY<STRUCT<ref_type STRING, value STRING>>,
    rate_total_evidence STRING,
    weight_evidence STRING,
    ocr_confidence FLOAT64,
    unverified_fields STRING
  >>
);

-- Final: one row per document found in a packet.
CREATE OR REPLACE TABLE `144240581301.freight.documents` (
  load_id STRING,
  source_pdf STRING,
  doc_type STRING,
  page_range STRING,
  illegible BOOL,
  is_signed_delivery_copy BOOL,
  delivery_signed_date DATE,
  load_number STRING,
  pro_number STRING,
  bol_number STRING,
  order_number STRING,
  rc_number STRING,
  broker_name STRING,
  broker_mc STRING,
  carrier_name STRING,
  carrier_mc STRING,
  carrier_dot STRING,
  carrier_scac STRING,
  shipper_name STRING,
  shipper_address STRING,
  consignee_name STRING,
  consignee_address STRING,
  origin_city STRING,
  origin_state STRING,
  origin_zip STRING,
  destination_city STRING,
  destination_state STRING,
  destination_zip STRING,
  pickup_date DATE,
  delivery_date DATE,
  pickup_appt_start STRING,
  pickup_appt_end STRING,
  delivery_appt_start STRING,
  delivery_appt_end STRING,
  rate_total NUMERIC,
  line_haul NUMERIC,
  fuel_surcharge NUMERIC,
  rate_is_all_in BOOL,
  currency STRING,
  miles FLOAT64,
  payment_net_days INT64,
  quick_pay_pct FLOAT64,
  commodity STRING,
  weight NUMERIC,
  weight_unit STRING,
  pieces INT64,
  pallet_count INT64,
  pallet_spaces INT64,
  equipment_class STRING,
  equipment_length_ft INT64,
  is_team BOOL,
  equipment_type_raw STRING,
  temperature_setpoint_f FLOAT64,
  temperature_mode STRING,
  freight_class STRING,
  hazmat BOOL,
  seal_number STRING,
  delivered BOOL,
  received_by STRING,
  signature_present BOOL,
  exceptions_notes STRING,
  pieces_received INT64,
  osd_code STRING,
  stops ARRAY<STRUCT<sequence INT64, stop_type STRING, name STRING, address STRING, city STRING, state STRING, zip STRING, scheduled_date STRING, appt_start STRING, appt_end STRING, appt_type STRING, reference_number STRING>>,
  accessorials ARRAY<STRUCT<type STRING, amount FLOAT64, notes STRING>>,
  references ARRAY<STRUCT<ref_type STRING, value STRING>>,
  rate_total_evidence STRING,
  weight_evidence STRING,
  ocr_confidence FLOAT64,
  unverified_fields STRING,
  needs_review BOOL,
  loaded_at TIMESTAMP
);


-- ========== SECTION B: flatten staging -> final ======================

-- Target columns are named EXPLICITLY. This INSERT used to rely on the SELECT list
-- lining up positionally across ~50 columns; it is now 72, and one inserted field
-- would shift every column after it, silently writing type-compatible garbage with no
-- error. Naming them removes that whole class of bug.
INSERT INTO `144240581301.freight.documents` (
  load_id, source_pdf, doc_type, page_range, illegible, is_signed_delivery_copy,
  delivery_signed_date, load_number, pro_number, bol_number, order_number, rc_number,
  broker_name, broker_mc, carrier_name, carrier_mc, carrier_dot, carrier_scac,
  shipper_name, shipper_address, consignee_name, consignee_address, origin_city,
  origin_state, origin_zip, destination_city, destination_state, destination_zip,
  pickup_date, delivery_date, pickup_appt_start, pickup_appt_end, delivery_appt_start,
  delivery_appt_end, rate_total, line_haul, fuel_surcharge, rate_is_all_in, currency,
  miles, payment_net_days, quick_pay_pct, commodity, weight, weight_unit, pieces,
  pallet_count, pallet_spaces, equipment_class, equipment_length_ft, is_team,
  equipment_type_raw, temperature_setpoint_f, temperature_mode, freight_class, hazmat,
  seal_number, delivered, received_by, signature_present, exceptions_notes,
  pieces_received, osd_code, stops, accessorials, references, rate_total_evidence,
  weight_evidence, ocr_confidence, unverified_fields, needs_review, loaded_at
)
SELECT
  s.load_id,
  s.source_pdf,
  d.doc_type,
  d.page_range,
  d.illegible,
  d.is_signed_delivery_copy,
  SAFE.PARSE_DATE('%Y-%m-%d', CAST(d.delivery_signed_date AS STRING)),
  d.load_number,
  d.pro_number,
  d.bol_number,
  d.order_number,
  d.rc_number,
  d.broker_name,
  d.broker_mc,
  d.carrier_name,
  d.carrier_mc,
  d.carrier_dot,
  d.carrier_scac,
  d.shipper_name,
  d.shipper_address,
  d.consignee_name,
  d.consignee_address,
  d.origin_city,
  d.origin_state,
  d.origin_zip,
  d.destination_city,
  d.destination_state,
  d.destination_zip,
  SAFE.PARSE_DATE('%Y-%m-%d', CAST(d.pickup_date AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', CAST(d.delivery_date AS STRING)),
  d.pickup_appt_start,
  d.pickup_appt_end,
  d.delivery_appt_start,
  d.delivery_appt_end,
  CAST(d.rate_total AS NUMERIC),
  CAST(d.line_haul AS NUMERIC),
  CAST(d.fuel_surcharge AS NUMERIC),
  d.rate_is_all_in,
  d.currency,
  d.miles,
  d.payment_net_days,
  d.quick_pay_pct,
  d.commodity,
  CAST(d.weight AS NUMERIC),
  d.weight_unit,
  d.pieces,
  d.pallet_count,
  d.pallet_spaces,
  d.equipment_class,
  d.equipment_length_ft,
  d.is_team,
  d.equipment_type_raw,
  d.temperature_setpoint_f,
  d.temperature_mode,
  d.freight_class,
  d.hazmat,
  d.seal_number,
  d.delivered,
  d.received_by,
  d.signature_present,
  d.exceptions_notes,
  d.pieces_received,
  d.osd_code,
  d.stops,
  d.accessorials,
  d.references,
  d.rate_total_evidence,
  d.weight_evidence,
  d.ocr_confidence,
  d.unverified_fields,
  (COALESCE(d.ocr_confidence, 0) < 0.92 OR d.illegible
     OR COALESCE(d.unverified_fields, '') != ''),
  CURRENT_TIMESTAMP()
FROM `144240581301.freight.stg_extractions` s, UNNEST(s.documents) d;


-- Final: one row per load (folder), fields rolled up across its documents.
-- Grouped by load_id — the packet folder — which is present on every row. See the
-- header of gold_layer.sql for why load_number is NOT used to tie documents together.
CREATE OR REPLACE TABLE `144240581301.freight.loads` AS
SELECT
  load_id,
  ANY_VALUE(source_pdf)                                            AS source_pdf,
  MAX(IF(doc_type = 'rate_confirmation', broker_name, NULL))       AS broker_name,
  MAX(carrier_name)                                                AS carrier_name,
  COALESCE(MAX(load_number), MAX(pro_number), MAX(bol_number))     AS reference_number,
  MAX(rate_total)                                                  AS rate_total,
  MAX(miles)                                                       AS miles,
  SAFE_DIVIDE(MAX(rate_total), NULLIF(MAX(miles), 0))              AS rate_per_mile,
  MAX(origin_city)      AS origin_city,      MAX(origin_state)      AS origin_state,
  MAX(destination_city) AS destination_city, MAX(destination_state) AS destination_state,
  MIN(pickup_date)      AS pickup_date,      MAX(delivery_date)     AS delivery_date,
  MAX(weight)           AS weight,
  -- the POD question, answerable at last: a signed BOL counts (see gold_layer.sql)
  LOGICAL_OR(COALESCE(is_signed_delivery_copy, FALSE)
             OR doc_type = 'proof_of_delivery')                     AS has_pod,
  MAX(delivery_signed_date)                                         AS delivery_signed_date,
  MIN(ocr_confidence)   AS worst_ocr_conf,
  COUNTIF(illegible)    AS illegible_docs,
  COUNT(*)              AS doc_count,
  COUNTIF(needs_review) AS docs_needing_review
FROM `144240581301.freight.documents`
GROUP BY load_id;


-- ========== Handy checks =============================================
-- How many loads processed, and how many need a human look:
--   SELECT COUNT(*) AS loads, SUM(docs_needing_review) AS docs_to_review
--   FROM `144240581301.freight.loads`;
--
-- POD coverage — this is the number that was 14% before is_signed_delivery_copy:
--   SELECT COUNTIF(has_pod) / COUNT(*) AS pod_rate FROM `144240581301.freight.loads`;
--
-- Least-reliable loads to review first (lowest OCR confidence at the top):
--   SELECT load_id, worst_ocr_conf, illegible_docs, docs_needing_review
--   FROM `144240581301.freight.loads` ORDER BY worst_ocr_conf ASC LIMIT 50;
--
-- DOUBLE-LOAD GUARD — must return zero rows. If it does not, staging was loaded
-- without --replace and every count downstream is inflated:
--   SELECT load_id, COUNT(*) n FROM `144240581301.freight.stg_extractions`
--   GROUP BY load_id HAVING n > 1 ORDER BY n DESC LIMIT 20;
--
-- Export for ML training (command line):
--   bq extract --destination_format=PARQUET \
--     144240581301:freight.loads "gs://<bucket>/exports/loads-*.parquet"
