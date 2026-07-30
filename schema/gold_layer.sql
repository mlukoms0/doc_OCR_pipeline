-- =====================================================================
-- GOLD LAYER — curated, per-document-type tables built from `freight.documents`.
--
-- Produces, in a NEW dataset `freight_gold`:
--   • documents_classified   (view)  — every doc + a normalized doc_class + carrier_key
--   • rate_confirmations      (table) — 1 row per RATE CON doc, all rate-con fields
--   • bills_of_lading         (table) — 1 row per BOL  doc, all BOL fields
--   • proofs_of_delivery      (table) — 1 row per POD  doc, all POD fields
--   • other_documents         (table) — everything else (packing/lumper/invoice/…), nothing dropped
--
-- THREE cross-checkable tables:
--   Each of the three per-type tables is one row per document and begins with the same
--   trace keys — load_id, load_number, source_pdf, page_range — so you can JOIN them by
--   load_number (and load_id) to reconcile a load across its documents. Example: does the
--   carrier / weight / delivery_date on the BOL and POD agree with the rate confirmation?
--
--     SELECT rc.load_number,
--            rc.carrier_name  AS rc_carrier,  b.carrier_name  AS bol_carrier,
--            rc.weight        AS rc_weight,    b.weight        AS bol_weight,
--            rc.delivery_date AS rc_delivery,  p.delivery_date AS pod_delivery,
--            p.delivered
--     FROM       `144240581301.freight_gold.rate_confirmations` rc
--     LEFT JOIN  `144240581301.freight_gold.bills_of_lading`    b USING (load_number)
--     LEFT JOIN  `144240581301.freight_gold.proofs_of_delivery` p USING (load_number);
--
--   (load_number is the SHARED reference printed on the RC, BOL, and POD. It is distinct
--    from rc_number, which is the rate confirmation's OWN document number and lives only
--    on the rate_confirmations table.)
--
-- Fixes baked in (from the pilot analysis):
--   1. doc_type normalized to a clean enum (source had 21 spellings) via documents_classified
--   2. carrier_key collapses the DB7 spellings so RC/BOL carriers line up on a join
--   3. broker_needs_verification flags rate cons whose broker is missing or low-trust
--
-- Replace 144240581301 with your project ID if BigQuery errors "Not found: Project".
-- Run once:  bq --location=US mk -d 144240581301:freight_gold      (or the CREATE SCHEMA below)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `144240581301.freight_gold` OPTIONS(location = 'US');


-- ---------------------------------------------------------------------
-- 0) Classification view — single source of truth for doc type + carrier key
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `144240581301.freight_gold.documents_classified` AS
SELECT
  *,
  CASE
    WHEN LOWER(doc_type) LIKE '%lading%'                                        THEN 'bill_of_lading'
    WHEN LOWER(doc_type) LIKE '%confirmation%' OR LOWER(doc_type) LIKE '%load sheet%'
                                                                                THEN 'rate_confirmation'
    WHEN LOWER(doc_type) LIKE '%proof of delivery%'
      OR LOWER(doc_type) LIKE '%delivery order%'
      OR REGEXP_CONTAINS(LOWER(doc_type), r'\bpod\b')                           THEN 'proof_of_delivery'
    WHEN LOWER(doc_type) LIKE '%packing%' OR LOWER(doc_type) LIKE '%manifest%'  THEN 'packing_list'
    WHEN LOWER(doc_type) LIKE '%lumper%'                                        THEN 'lumper_receipt'
    WHEN LOWER(doc_type) LIKE '%invoice%'                                       THEN 'invoice'
    ELSE 'other'
  END AS doc_class,
  -- Canonical carrier key: uppercase, drop THE/LLC/INC/CO/CORP/COMPANY + punctuation.
  -- Collapses "THE DB7 COMPANY, LLC" / "The DB7 Company LLC" / "THE DB7 COMPANY" -> "DB7".
  -- Approximate (won't fix rep-name captures like "Vitali at DB7") — good enough for joins.
  REGEXP_REPLACE(
    REGEXP_REPLACE(UPPER(COALESCE(carrier_name, '')),
                   r'\b(THE|LLC|L\.L\.C|INC|CORP|CO|COMPANY|TRUCKING)\b', ''),
    r'[^A-Z0-9]', ''
  ) AS carrier_key
FROM `144240581301.freight.documents`;


-- ---------------------------------------------------------------------
-- 1) Rate Confirmations — 1 row per rate-con document
--    rc_number is the rate con's OWN number; load_number is the shared join key.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.rate_confirmations` AS
SELECT
  -- join / trace keys (shared across the three tables)
  load_id, load_number, source_pdf, page_range,
  -- rate-con identity + broker
  rc_number,
  broker_name,
  (broker_name IS NULL OR needs_review) AS broker_needs_verification,
  broker_mc,
  -- carrier
  carrier_name, carrier_key, carrier_mc, carrier_dot, carrier_scac,
  -- lane
  origin_city, origin_state, origin_zip,
  destination_city, destination_state, destination_zip,
  -- dates
  pickup_date, delivery_date,
  -- money
  rate_total, line_haul, fuel_surcharge, currency,
  -- freight
  commodity, weight, weight_unit, pallet_count, pallet_spaces, pieces, equipment_type,
  -- quality / trace
  confidence, ocr_confidence, unverified_fields, needs_review
FROM `144240581301.freight_gold.documents_classified`
WHERE doc_class = 'rate_confirmation';


-- ---------------------------------------------------------------------
-- 2) Bills of Lading — 1 row per BOL document
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.bills_of_lading` AS
SELECT
  -- join / trace keys (shared across the three tables)
  load_id, load_number, source_pdf, page_range,
  -- BOL identifiers
  bol_number, pro_number, order_number,
  -- parties
  shipper_name, shipper_address, consignee_name, consignee_address,
  -- carrier
  carrier_name, carrier_key, carrier_scac,
  -- lane
  origin_city, origin_state, origin_zip,
  destination_city, destination_state, destination_zip,
  -- dates
  pickup_date, delivery_date,
  -- freight
  commodity, weight, weight_unit, pieces, pallet_count, freight_class, hazmat,
  -- quality / trace
  confidence, ocr_confidence, unverified_fields, needs_review
FROM `144240581301.freight_gold.documents_classified`
WHERE doc_class = 'bill_of_lading';


-- ---------------------------------------------------------------------
-- 3) Proofs of Delivery — 1 row per POD document
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.proofs_of_delivery` AS
SELECT
  -- join / trace keys (shared across the three tables)
  load_id, load_number, source_pdf, page_range,
  -- delivery outcome
  delivered, received_by, signature_present, delivery_date, exceptions_notes,
  -- freight actually delivered
  pieces, weight,
  -- quality / trace
  confidence, ocr_confidence, needs_review
FROM `144240581301.freight_gold.documents_classified`
WHERE doc_class = 'proof_of_delivery';


-- ---------------------------------------------------------------------
-- 4) Other documents (packing slips, lumper receipts, invoices, terms, certs) — kept, not dropped
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.other_documents` AS
SELECT
  -- join / trace keys
  load_id, load_number, source_pdf, page_range,
  -- what it is
  doc_type, doc_class,
  -- whatever identifiers / context these misc docs carry (nothing dropped)
  pro_number, bol_number, order_number, rc_number,
  carrier_name, carrier_key,
  shipper_name, consignee_name,
  origin_city, origin_state, destination_city, destination_state,
  pickup_date, delivery_date,
  commodity, weight, weight_unit, pieces, pallet_count, pallet_spaces,
  exceptions_notes,
  -- quality / trace
  confidence, ocr_confidence, unverified_fields, needs_review
FROM `144240581301.freight_gold.documents_classified`
WHERE doc_class NOT IN ('rate_confirmation', 'bill_of_lading', 'proof_of_delivery');


-- ---------------------------------------------------------------------
-- Sanity checks (run as needed):
--   -- doc mix:
--   SELECT doc_class, COUNT(*) FROM `144240581301.freight_gold.documents_classified` GROUP BY 1 ORDER BY 2 DESC;
--
--   -- row counts per gold table:
--   SELECT 'rate_confirmations' t, COUNT(*) n FROM `144240581301.freight_gold.rate_confirmations`
--   UNION ALL SELECT 'bills_of_lading',    COUNT(*) FROM `144240581301.freight_gold.bills_of_lading`
--   UNION ALL SELECT 'proofs_of_delivery', COUNT(*) FROM `144240581301.freight_gold.proofs_of_delivery`
--   UNION ALL SELECT 'other_documents',    COUNT(*) FROM `144240581301.freight_gold.other_documents`;
--
--   -- CROSS-CHECK the three tables by load_number (does the load agree across its docs?):
--   SELECT rc.load_number,
--          rc.carrier_key  AS rc_carrier,  b.carrier_key  AS bol_carrier,
--          rc.weight       AS rc_weight,    b.weight       AS bol_weight,
--          rc.delivery_date AS rc_delivery, p.delivery_date AS pod_delivery, p.delivered
--   FROM       `144240581301.freight_gold.rate_confirmations` rc
--   LEFT JOIN  `144240581301.freight_gold.bills_of_lading`    b USING (load_number)
--   LEFT JOIN  `144240581301.freight_gold.proofs_of_delivery` p USING (load_number)
--   WHERE rc.carrier_key != b.carrier_key OR rc.weight != b.weight;   -- mismatches to review
--
--   -- rate cons whose broker needs a human look:
--   SELECT load_number, rc_number, broker_name, needs_review
--   FROM `144240581301.freight_gold.rate_confirmations`
--   WHERE broker_needs_verification;
-- ---------------------------------------------------------------------
