-- =====================================================================
-- BigQuery setup for the freight extraction project.
-- Project is set to 144240581301 below.
--   NOTE: BigQuery SQL and bq want the project *ID* (e.g. "linic-freight-123"),
--   not the numeric project *number*. If a query errors with
--   "Not found: Project 144240581301", Find & Replace it with your project ID.
-- The dataset is called `freight` and lives in the US multi-region.
--
-- Order of operations:
--   1) Create the dataset (command line):
--        bq --location=US mk -d 144240581301:freight
--   2) Run section A below to create the tables.
--   3) After extract.py has written JSON to gs://<doc-archive67>/json/, load it
--      (command line):
--        bq load --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values \
--          144240581301:freight.stg_extractions "gs://<doc-archive67>/json/*.json"
--   4) Run section B to flatten into the final tables.
-- =====================================================================


-- ========== SECTION A: create tables =================================

-- Staging table: mirrors exactly what extract.py writes (one row per PDF).
-- Dates and money are loaded as STRING/FLOAT here and cleaned up in Section B.
CREATE TABLE IF NOT EXISTS `144240581301.freight.stg_extractions` (
  load_id STRING,
  source_pdf STRING,
  packet_summary STRING,
  documents ARRAY<STRUCT<
    doc_type STRING, page_range STRING, confidence FLOAT64, ocr_confidence FLOAT64, unverified_fields STRING, illegible BOOL,
    load_number STRING, pro_number STRING, bol_number STRING, order_number STRING, rc_number STRING,
    broker_name STRING, broker_mc STRING,
    carrier_name STRING, carrier_mc STRING, carrier_dot STRING, carrier_scac STRING,
    shipper_name STRING, shipper_address STRING,
    consignee_name STRING, consignee_address STRING,
    origin_city STRING, origin_state STRING, origin_zip STRING,
    destination_city STRING, destination_state STRING, destination_zip STRING,
    pickup_date STRING, delivery_date STRING,
    rate_total FLOAT64, line_haul FLOAT64, fuel_surcharge FLOAT64, currency STRING,
    commodity STRING, weight FLOAT64, weight_unit STRING, pieces INT64, pallet_count INT64, pallet_spaces INT64,
    equipment_type STRING, freight_class STRING, hazmat BOOL,
    delivered BOOL, received_by STRING, signature_present BOOL, exceptions_notes STRING
  >>
);

-- Final: one row per document found in a packet.
CREATE TABLE IF NOT EXISTS `144240581301.freight.documents` (
  load_id STRING,
  source_pdf STRING,
  doc_type STRING,
  page_range STRING,
  confidence FLOAT64,
  ocr_confidence FLOAT64,
  unverified_fields STRING,
  needs_review BOOL,
  load_number STRING, pro_number STRING, bol_number STRING, order_number STRING, rc_number STRING,
  broker_name STRING, broker_mc STRING,
  carrier_name STRING, carrier_mc STRING, carrier_dot STRING, carrier_scac STRING,
  shipper_name STRING, shipper_address STRING,
  consignee_name STRING, consignee_address STRING,
  origin_city STRING, origin_state STRING, origin_zip STRING,
  destination_city STRING, destination_state STRING, destination_zip STRING,
  pickup_date DATE, delivery_date DATE,
  rate_total NUMERIC, line_haul NUMERIC, fuel_surcharge NUMERIC, currency STRING,
  commodity STRING, weight NUMERIC, weight_unit STRING, pieces INT64, pallet_count INT64, pallet_spaces INT64,
  equipment_type STRING, freight_class STRING, hazmat BOOL,
  delivered BOOL, received_by STRING, signature_present BOOL, exceptions_notes STRING,
  loaded_at TIMESTAMP
);


-- ========== SECTION B: flatten staging -> final ======================

-- Wipe the final table so this step is safe to re-run.
TRUNCATE TABLE `144240581301.freight.documents`;

INSERT INTO `144240581301.freight.documents`
SELECT
  s.load_id,
  s.source_pdf,
  d.doc_type,
  d.page_range,
  d.confidence,
  d.ocr_confidence,
  d.unverified_fields,
  (d.confidence < 0.85 OR COALESCE(d.ocr_confidence, 0) < 0.92 OR d.illegible
     OR COALESCE(d.unverified_fields, '') != '') AS needs_review,  -- tightened: ocr<0.92, gemini<0.85
  d.load_number, d.pro_number, d.bol_number, d.order_number, d.rc_number,
  d.broker_name, d.broker_mc,
  d.carrier_name, d.carrier_mc, d.carrier_dot, d.carrier_scac,
  d.shipper_name, d.shipper_address,
  d.consignee_name, d.consignee_address,
  d.origin_city, d.origin_state, d.origin_zip,
  d.destination_city, d.destination_state, d.destination_zip,
  SAFE.PARSE_DATE('%Y-%m-%d', CAST(d.pickup_date AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', CAST(d.delivery_date AS STRING)),
  CAST(d.rate_total AS NUMERIC), CAST(d.line_haul AS NUMERIC), CAST(d.fuel_surcharge AS NUMERIC), d.currency,
  d.commodity, CAST(d.weight AS NUMERIC), d.weight_unit, d.pieces, d.pallet_count, d.pallet_spaces,
  d.equipment_type, d.freight_class, d.hazmat,
  d.delivered, d.received_by, d.signature_present, d.exceptions_notes,
  CURRENT_TIMESTAMP()
FROM `144240581301.freight.stg_extractions` s, UNNEST(s.documents) d;


-- Final: one row per load (folder), fields rolled up across its documents.
-- This is a starter rollup — refine which document type each field comes from later.
CREATE OR REPLACE TABLE `144240581301.freight.loads` AS
SELECT
  load_id,
  ANY_VALUE(source_pdf)                                            AS source_pdf,
  MAX(IF(doc_type = 'rate_confirmation', broker_name, NULL))       AS broker_name,
  COALESCE(MAX(carrier_name))                                      AS carrier_name,
  COALESCE(MAX(load_number), MAX(pro_number), MAX(bol_number))     AS reference_number,
  MAX(rate_total)                                                  AS rate_total,
  MAX(origin_city)     AS origin_city,     MAX(origin_state)      AS origin_state,
  MAX(destination_city) AS destination_city, MAX(destination_state) AS destination_state,
  MIN(pickup_date)     AS pickup_date,     MAX(delivery_date)     AS delivery_date,
  MAX(weight)          AS weight,
  MIN(ocr_confidence)  AS worst_ocr_conf,       -- lowest OCR confidence across the load's docs
  MIN(confidence)      AS worst_gemini_conf,    -- lowest Gemini confidence across the load's docs
  COUNT(*)             AS doc_count,
  COUNTIF(needs_review) AS docs_needing_review
FROM `144240581301.freight.documents`
GROUP BY load_id;


-- ========== Handy checks =============================================
-- How many loads processed, and how many need a human look:
--   SELECT COUNT(*) AS loads,
--          SUM(docs_needing_review) AS docs_to_review
--   FROM `144240581301.freight.loads`;
--
-- Least-reliable loads to review first (lowest OCR confidence at the top):
--   SELECT load_id, worst_ocr_conf, worst_gemini_conf, docs_needing_review
--   FROM `144240581301.freight.loads`
--   ORDER BY worst_ocr_conf ASC
--   LIMIT 50;
--
-- Export the load table for ML training (command line):
--   bq extract --destination_format=PARQUET \
--     144240581301:freight.loads "gs://<bucket>/exports/loads-*.parquet"
