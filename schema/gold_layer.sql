-- =====================================================================
-- GOLD LAYER — curated, per-document-type tables built from `freight.documents`.
--
-- Produces, in a NEW dataset `freight_gold`:
--   • documents_classified   (view)  — every doc + normalized doc_class, carrier_key,
--                                      plausibility-clamped pallets, date_suspect
--   • load_reference         (view)  — ONE row per load: the packet's load_number and
--                                      its consolidated pallet figures
--   • rate_confirmations      (table) — 1 row per RATE CON doc, all rate-con fields
--   • bills_of_lading         (table) — 1 row per BOL  doc, all BOL fields
--   • proofs_of_delivery      (table) — 1 row per POD  doc, all POD fields
--   • other_documents         (table) — everything else (packing/lumper/invoice/…), nothing dropped
--
-- =====================================================================
-- JOIN ON load_id — NOT load_number.
-- =====================================================================
-- load_id is the packet folder. One folder = one packet = one load, and it is present
-- on 100% of rows. load_number is a number READ OFF THE PAGE, and measured on the
-- 1,000-load run it is not fit to be a key:
--
--                                   join on load_number   join on load_id
--   rate cons matched to their BOL      869/1209 (72%)     1188/1209 (98%)
--   rows joined to a DIFFERENT load's
--     BOL (silent wrong data)                        6                   0
--
--   • load_number is NULL on 25.6% of BOLs — those documents cannot join at all.
--   • Three number pairs collide across unrelated folders (27522/27523,
--     25818/26763, 26684/26685), quietly cross-joining two different loads.
--
-- So load_number is now a CROSS-CHECK, not a key. Every gold table carries both:
--   load_id             — join on this
--   packet_load_number  — the load's number, resolved once per packet from the rate
--                         confirmation (see load_reference); use it to report/track
--   load_number         — what THIS document printed, for reconciliation
--   load_number_conflict— TRUE when the packet's documents disagree on the number
--
-- Extraction no longer copies values between documents (each document is read from
-- its own pages only), so cross-document fill happens HERE, where it is visible and
-- auditable, instead of inside the model where it was indistinguishable from a
-- hallucination — that one behaviour was generating 48% of the BOL review queue.
--
--     SELECT rc.packet_load_number,
--            rc.carrier_key  AS rc_carrier,  b.carrier_key  AS bol_carrier,
--            rc.weight       AS rc_weight,   b.weight       AS bol_weight,
--            rc.delivery_date AS rc_delivery, p.delivery_date AS pod_delivery,
--            p.delivered
--     FROM       `144240581301.freight_gold.rate_confirmations` rc
--     LEFT JOIN  `144240581301.freight_gold.bills_of_lading`    b USING (load_id)
--     LEFT JOIN  `144240581301.freight_gold.proofs_of_delivery` p USING (load_id);
--
-- Fixes baked in (from the pilot + 1,000-load analysis):
--   1. doc_type normalized to a clean enum (source had 21 spellings) via documents_classified
--   2. carrier_key collapses the DB7 spellings so RC/BOL carriers line up on a join
--   3. joins are on load_id; load_number demoted to a cross-check (see above)
--   4. rate_confirmations fills load_number/rc_number BOTH ways (one broker number serves
--      both on most RCs); pallet figures consolidated per load in load_reference
--   5. proof_of_delivery classifier also matches the snake_case enum value
--   6. pallets clamped to a physically possible 1-60 IN THE VIEW, so every downstream
--      table inherits it (silver had counts up to 3,165 — those are carton counts)
--   7. "Delivery Order" no longer counts as a POD — a D/O is a cargo-release instruction
--      (ocean/drayage), not evidence that anything was delivered
--   8. "Bill of Lading Supplement" no longer creates a phantom second BOL per load
--   9. rate_breakdown_conflict flags line_haul + fuel_surcharge != rate_total
--  10. broker_needs_verification REMOVED — it was (broker_name IS NULL OR needs_review),
--      and since broker_name is 99.7% populated it just restated needs_review under a
--      misleading name: 97% of the rows it flagged had a broker, and none of them had a
--      broker-related entry in unverified_fields. Use needs_review.
--
-- Replace 144240581301 with your project ID if BigQuery errors "Not found: Project".
-- Run once:  bq --location=US mk -d 144240581301:freight_gold      (or the CREATE SCHEMA below)
-- Run AFTER bigquery_setup.sql section B has rebuilt `freight.documents`.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `144240581301.freight_gold` OPTIONS(location = 'US');


-- ---------------------------------------------------------------------
-- 0) Classification view — single source of truth for doc type, carrier key,
--    pallet plausibility and date plausibility.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `144240581301.freight_gold.documents_classified` AS
SELECT
  * EXCEPT (pallet_count, pallet_spaces),

  CASE
    -- a BOL "supplement"/continuation page is part of a BOL, not a second BOL:
    -- routing it to 'other' stops it fanning out the bills_of_lading table
    WHEN LOWER(doc_type) LIKE '%lading%'
     AND LOWER(doc_type) NOT LIKE '%supplement%'                                THEN 'bill_of_lading'
    WHEN LOWER(doc_type) LIKE '%confirmation%' OR LOWER(doc_type) LIKE '%load sheet%'
                                                                                THEN 'rate_confirmation'
    -- NOTE: '%delivery order%' is deliberately NOT here. A Delivery Order is an
    -- instruction to release cargo (ocean/drayage), not a proof of delivery.
    WHEN LOWER(doc_type) = 'proof_of_delivery'
      OR LOWER(doc_type) LIKE '%proof of delivery%'
      OR REGEXP_CONTAINS(LOWER(doc_type), r'\bpod\b')                           THEN 'proof_of_delivery'
    WHEN LOWER(doc_type) LIKE '%packing%' OR LOWER(doc_type) LIKE '%manifest%'  THEN 'packing_list'
    WHEN LOWER(doc_type) LIKE '%lumper%'                                        THEN 'lumper_receipt'
    WHEN LOWER(doc_type) LIKE '%invoice%'                                       THEN 'invoice'
    ELSE 'other'
  END AS doc_class,

  -- Canonical carrier key: uppercase, strip rep names / suffixes / punctuation.
  -- Collapses "THE DB7 COMPANY, LLC" / "The DB7 Company LLC" / "THE DB7 COMPANY" -> "DB7".
  -- Three fixes over the first version, all from spellings seen in the 1,000-load run:
  --   * "Dmitriy Bruyaka at DB7" (7 rows) — everything up to and including " AT " goes
  --   * "DB SEVEN" / "DB SEVEN NC" (43 rows) — spelled-out number normalized to 7
  --   * "DB7 LIC" (4 rows) — LIC is an OCR misread of LLC
  -- Deliberately NOT stripping TRANSPORT/LOGISTICS: that would collapse genuinely
  -- different carriers ("ABC Transport" vs "ABC Logistics") into one key.
  --   * "DB SEVEN NC" — a trailing domicile-state code, stripped against the REAL state
  --     list so a meaningful trailing token can never be eaten by accident
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(COALESCE(carrier_name, '')), r'^.*?\s+AT\s+', ''),
          r'\bSEVEN\b', '7'),
        r'\b(THE|LLC|LIC|L\.L\.C|LTD|LP|INC|CORP|CO|COMPANY|TRUCKING)\b', ''),
      r'[\s,\.]+(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)[\s,\.]*$', ''),
    r'[^A-Z0-9]', ''
  ) AS carrier_key,

  -- THE POD FIX (issue 2.1). A signed BOL IS the proof of delivery — same sheet,
  -- stamped at the consignee. Treating doc_type as the only signal put POD coverage at
  -- 14% of loads, when a carrier cannot invoice without one. Role, not type:
  COALESCE(is_signed_delivery_copy, FALSE)
    OR LOWER(doc_type) = 'proof_of_delivery'
    OR LOWER(doc_type) LIKE '%proof of delivery%'                    AS is_pod,

  -- Pallet plausibility. A trailer holds at most ~60 positions double-stacked, so a
  -- count in the hundreds or thousands is a carton count misfiled into a pallet field.
  -- Clamped HERE rather than in one table so every downstream consumer inherits it;
  -- the raw values are kept beside them so nothing is lost and the miss can be audited.
  pallet_count                                            AS pallet_count_raw,
  pallet_spaces                                           AS pallet_spaces_raw,
  IF(pallet_count  BETWEEN 1 AND 60, pallet_count,  NULL) AS pallet_count,
  IF(pallet_spaces BETWEEN 1 AND 60, pallet_spaces, NULL) AS pallet_spaces,

  -- Weight in a single unit. 19 documents in the 500-packet run were in KILOGRAMS, and
  -- anything downstream that assumes pounds is wrong by 2.2x on those. weight/weight_unit
  -- stay raw; weight_lb is the one to compute with. A null unit is treated as pounds
  -- (the corpus is US domestic) but flagged so the assumption is visible.
  CASE LOWER(weight_unit)
    WHEN 'kg' THEN ROUND(weight * 2.20462, 0)
    ELSE weight
  END                                         AS weight_lb,
  (weight IS NOT NULL AND weight_unit IS NULL) AS weight_unit_assumed,

  -- Date plausibility. Dates are NOT nulled — the raw value stays, this only flags.
  -- (Seen in the pilot: a 2002 pickup date and an 1,854-day transit on BOLs, both
  --  year misreads on faded scans.)
  (   COALESCE(delivery_date < pickup_date, FALSE)
   OR COALESCE(DATE_DIFF(delivery_date, pickup_date, DAY) > 60, FALSE)
   OR COALESCE(EXTRACT(YEAR FROM pickup_date)   NOT BETWEEN 2015 AND 2027, FALSE)
   OR COALESCE(EXTRACT(YEAR FROM delivery_date) NOT BETWEEN 2015 AND 2027, FALSE)
  ) AS date_suspect

FROM `144240581301.freight.documents`;


-- ---------------------------------------------------------------------
-- 0b) Load reference — ONE row per packet. This is where cross-document fill lives.
--     packet_load_number: the load's number, taken from the rate confirmation (the
--     cleanest, most standardized document in the packet) and falling back to its
--     rc_number, then to any document that printed one.
--     Pallet figures are consolidated per load: BOL preferred for the physical COUNT
--     (the BOL records what was actually put on the truck), rate con preferred for
--     SPACES (only rate cons normally quote trailer positions).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `144240581301.freight_gold.load_reference` AS
SELECT
  load_id,
  COALESCE(
    MAX(IF(doc_class = 'rate_confirmation', load_number, NULL)),
    MAX(IF(doc_class = 'rate_confirmation', rc_number,   NULL)),
    MAX(load_number)
  ) AS packet_load_number,
  COUNT(DISTINCT load_number) > 1 AS load_number_conflict,   -- documents disagree
  COALESCE(
    MAX(IF(doc_class = 'bill_of_lading',    pallet_count, NULL)),
    MAX(IF(doc_class = 'rate_confirmation', pallet_count, NULL)),
    MAX(pallet_count)
  ) AS load_pallet_count,
  COALESCE(
    MAX(IF(doc_class = 'rate_confirmation', pallet_spaces, NULL)),
    MAX(pallet_spaces)
  ) AS load_pallet_spaces,
  COUNT(DISTINCT pallet_count)  > 1 AS pallet_count_conflict,
  COUNT(DISTINCT pallet_spaces) > 1 AS pallet_spaces_conflict
FROM `144240581301.freight_gold.documents_classified`
GROUP BY load_id;


-- ---------------------------------------------------------------------
-- 1) Rate Confirmations — 1 row per rate-con document
--    rc_number = the rate con's identifying number; on most RCs it is the SAME single
--    number as load_number, so both are filled via COALESCE (they differ only when the
--    doc prints two distinct numbers).
--    pallet_count / pallet_spaces are THIS DOCUMENT's values. load_pallet_count /
--    load_pallet_spaces are the load-level consolidated figures — named differently on
--    purpose, because identical names for two different grains in one row is how a
--    document-level number silently gets read as a load-level one.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.rate_confirmations` AS
SELECT
  -- join / trace keys (shared across the three tables)
  d.load_id,                                             -- <- JOIN ON THIS
  r.packet_load_number,
  COALESCE(d.load_number, d.rc_number) AS load_number,   -- what THIS doc printed
  r.load_number_conflict,
  d.source_pdf, d.page_range,
  -- rate-con identity + broker
  COALESCE(d.rc_number, d.load_number) AS rc_number,      -- most RCs print one number serving both
  d.broker_name, d.broker_mc,
  -- carrier
  d.carrier_name, d.carrier_key, d.carrier_mc, d.carrier_dot, d.carrier_scac,
  -- lane
  d.origin_city, d.origin_state, d.origin_zip,
  d.destination_city, d.destination_state, d.destination_zip,
  -- dates + appointment windows (dates alone cannot answer detention or on-time)
  d.pickup_date, d.delivery_date, d.date_suspect,
  d.pickup_appt_start, d.pickup_appt_end,
  d.delivery_appt_start, d.delivery_appt_end,
  -- money (raw as extracted)
  d.rate_total, d.line_haul, d.fuel_surcharge, d.rate_is_all_in, d.currency,
  -- ...and the DERIVED truth. Measured on the 500-packet run: the model set
  -- rate_is_all_in = FALSE on 300 rate cons, but on 210 of those it then put the ALL-IN
  -- TOTAL into line_haul. An asserted breakdown whose line haul equals the total is not
  -- a breakdown. Use these two columns for anything that cares about rate structure;
  -- the raw values above are kept so the miss stays auditable.
  (COALESCE(d.rate_is_all_in, FALSE) OR d.line_haul = d.rate_total) AS rate_is_all_in_derived,
  IF(d.line_haul = d.rate_total, NULL, d.line_haul)                 AS line_haul_clean,
  (d.line_haul = d.rate_total AND d.rate_is_all_in IS FALSE)        AS line_haul_was_total,
  d.miles,
  SAFE_DIVIDE(d.rate_total, NULLIF(d.miles, 0)) AS rate_per_mile,   -- THE freight metric
  d.payment_net_days, d.quick_pay_pct,
  -- accessorials rolled up per document; the line items are in freight_gold.accessorials
  (SELECT COALESCE(SUM(a.amount), 0) FROM UNNEST(d.accessorials) a) AS accessorial_total,
  ARRAY_LENGTH(d.accessorials) AS accessorial_count,
  ARRAY_LENGTH(d.stops)        AS stop_count,
  d.rate_total_evidence,
  -- A printed breakdown must add up. On the 1,000-load run, 68% of the rate cons that
  -- had BOTH line_haul and fuel_surcharge did not sum to rate_total — almost all of
  -- them because the all-in total had been copied into line_haul AND a fuel figure
  -- picked up separately. Treat line_haul as unusable wherever this is TRUE.
  ( d.line_haul IS NOT NULL AND d.fuel_surcharge IS NOT NULL AND d.rate_total IS NOT NULL
    AND ABS(d.line_haul + d.fuel_surcharge - d.rate_total) > 1 ) AS rate_breakdown_conflict,
  -- freight — document-level
  d.commodity, d.weight, d.weight_unit, d.weight_lb, d.weight_unit_assumed, d.weight_evidence,
  d.pallet_count, d.pallet_spaces, d.pallet_count_raw, d.pallet_spaces_raw,
  d.pieces,
  -- equipment, split out of the free-text field that had 100+ spellings of one concept
  d.equipment_class, d.equipment_length_ft, d.is_team, d.equipment_type_raw,
  d.temperature_setpoint_f, d.temperature_mode,
  -- freight — load-level consolidated (from ANY of the packet's documents)
  r.load_pallet_count, r.load_pallet_spaces,
  r.pallet_count_conflict, r.pallet_spaces_conflict,
  -- quality / trace
  d.ocr_confidence, d.unverified_fields, d.illegible, d.needs_review
FROM `144240581301.freight_gold.documents_classified` d
LEFT JOIN `144240581301.freight_gold.load_reference` r USING (load_id)
WHERE d.doc_class = 'rate_confirmation';


-- ---------------------------------------------------------------------
-- 2) Bills of Lading — 1 row per BOL document
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.bills_of_lading` AS
SELECT
  -- join / trace keys (shared across the three tables)
  d.load_id,                                             -- <- JOIN ON THIS
  r.packet_load_number,
  d.load_number,                                         -- what THIS doc printed (often null)
  r.load_number_conflict,
  d.source_pdf, d.page_range,
  -- BOL identifiers
  d.bol_number, d.pro_number, d.order_number,
  -- parties
  d.shipper_name, d.shipper_address, d.consignee_name, d.consignee_address,
  -- carrier
  d.carrier_name, d.carrier_key, d.carrier_scac,
  -- lane
  d.origin_city, d.origin_state, d.origin_zip,
  d.destination_city, d.destination_state, d.destination_zip,
  -- dates
  d.pickup_date, d.delivery_date, d.date_suspect,
  -- POD role: a signed BOL is also a proof of delivery (it appears in both tables)
  d.is_signed_delivery_copy, d.delivery_signed_date, d.received_by, d.signature_present,
  -- freight (pallet_count is clamped 1-60 by the view; raw kept for audit)
  d.commodity, d.weight, d.weight_unit, d.weight_lb, d.weight_unit_assumed,
  d.pieces, d.pieces_received, d.osd_code,
  d.pallet_count, d.pallet_count_raw, d.freight_class, d.hazmat, d.seal_number,
  -- quality / trace
  d.ocr_confidence, d.unverified_fields, d.illegible, d.needs_review
FROM `144240581301.freight_gold.documents_classified` d
LEFT JOIN `144240581301.freight_gold.load_reference` r USING (load_id)
WHERE d.doc_class = 'bill_of_lading';


-- ---------------------------------------------------------------------
-- 3) Proofs of Delivery — 1 row per POD document
-- ---------------------------------------------------------------------
-- A row here is any document that PROVES DELIVERY — which in a freight packet is
-- usually the bill of lading, signed at the consignee. It therefore appears in BOTH
-- this table and bills_of_lading, on purpose: it genuinely is both documents.
-- `source_doc_class` tells you which physical form it took.
CREATE OR REPLACE TABLE `144240581301.freight_gold.proofs_of_delivery` AS
SELECT
  -- join / trace keys (shared across the three tables)
  d.load_id,                                             -- <- JOIN ON THIS
  r.packet_load_number,
  d.load_number,
  d.source_pdf, d.page_range,
  d.doc_class AS source_doc_class,                       -- 'bill_of_lading' for a signed BOL
  d.is_signed_delivery_copy,
  -- delivery outcome
  d.delivered, d.received_by, d.signature_present,
  COALESCE(d.delivery_signed_date, d.delivery_date) AS delivery_date,
  d.delivery_signed_date, d.date_suspect, d.exceptions_notes,
  -- what was actually received vs what shipped — the gap IS the claim
  d.pieces, d.pieces_received, d.osd_code,
  (d.pieces IS NOT NULL AND d.pieces_received IS NOT NULL
     AND d.pieces_received != d.pieces)                  AS piece_count_short,
  d.weight, d.weight_unit, d.weight_lb,
  -- quality / trace
  d.ocr_confidence, d.unverified_fields, d.illegible, d.needs_review
FROM `144240581301.freight_gold.documents_classified` d
LEFT JOIN `144240581301.freight_gold.load_reference` r USING (load_id)
WHERE d.is_pod;


-- ---------------------------------------------------------------------
-- 4) Other documents (packing slips, lumper receipts, invoices, terms, certs) — kept, not dropped
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.other_documents` AS
SELECT
  -- join / trace keys
  d.load_id,                                             -- <- JOIN ON THIS
  r.packet_load_number,
  d.load_number,
  d.source_pdf, d.page_range,
  -- what it is
  d.doc_type, d.doc_class,
  -- whatever identifiers / context these misc docs carry (nothing dropped)
  d.pro_number, d.bol_number, d.order_number, d.rc_number,
  d.carrier_name, d.carrier_key,
  d.shipper_name, d.consignee_name,
  d.origin_city, d.origin_state, d.destination_city, d.destination_state,
  d.pickup_date, d.delivery_date, d.date_suspect,
  d.commodity, d.weight, d.weight_unit, d.weight_lb, d.pieces, d.pallet_count, d.pallet_spaces,
  d.exceptions_notes,
  -- quality / trace
  d.ocr_confidence, d.unverified_fields, d.illegible, d.needs_review
FROM `144240581301.freight_gold.documents_classified` d
LEFT JOIN `144240581301.freight_gold.load_reference` r USING (load_id)
WHERE d.doc_class NOT IN ('rate_confirmation', 'bill_of_lading', 'proof_of_delivery');


-- ---------------------------------------------------------------------
-- 5) Stops — the FULL itinerary, one row per stop, unnested from documents.
--    origin_*/destination_* on the parent tables are only the first pickup and the
--    last delivery; every intermediate stop on a multi-stop load lived nowhere before.
-- ---------------------------------------------------------------------
-- Every document reports its OWN itinerary, and the BOL always names shipper and
-- consignee, so a plain unnest gives a 2-stop load 4+ rows. On the 500-packet run 66% of
-- loads showed more than 2 stops, which is not plausible for real multi-stop freight.
-- Nothing is dropped: every row is kept, and is_primary_itinerary marks the ONE document
-- per load whose stop list should be treated as the load's route. The rate confirmation
-- wins because it is the document that actually lists intermediate stops; the BOL is the
-- fallback; longest list breaks a tie.
--   the load's route  ->  WHERE is_primary_itinerary
--   cross-check       ->  compare the others against it
CREATE OR REPLACE TABLE `144240581301.freight_gold.stops` AS
WITH ranked AS (
  SELECT
    d.load_id, d.doc_class, d.page_range, d.stops, d.needs_review,
    ROW_NUMBER() OVER (
      PARTITION BY d.load_id
      ORDER BY CASE d.doc_class
                 WHEN 'rate_confirmation' THEN 1
                 WHEN 'bill_of_lading'    THEN 2
                 ELSE 3 END,
               ARRAY_LENGTH(d.stops) DESC,
               d.page_range
    ) AS doc_rank
  FROM `144240581301.freight_gold.documents_classified` d
  WHERE ARRAY_LENGTH(d.stops) > 0
)
SELECT
  k.load_id,
  r.packet_load_number,
  (k.doc_rank = 1) AS is_primary_itinerary,
  k.doc_class, k.page_range,
  s.sequence, s.stop_type,
  s.name, s.address, s.city, s.state, s.zip,
  SAFE.PARSE_DATE('%Y-%m-%d', s.scheduled_date) AS scheduled_date,
  s.appt_start, s.appt_end, s.appt_type,
  s.reference_number,
  k.needs_review
FROM ranked k,
     UNNEST(k.stops) s
LEFT JOIN `144240581301.freight_gold.load_reference` r ON r.load_id = k.load_id;


-- ---------------------------------------------------------------------
-- 6) Accessorials — one row per charge beyond the line haul. This is carrier revenue
--    that the original schema could not see at all: detention, layover, TONU, lumper,
--    stop-off, driver assist, tarp, reconsignment.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `144240581301.freight_gold.accessorials` AS
SELECT
  d.load_id,
  r.packet_load_number,
  d.doc_class, d.page_range,
  a.type AS accessorial_type,
  CAST(a.amount AS NUMERIC) AS amount,
  a.notes,
  d.needs_review
FROM `144240581301.freight_gold.documents_classified` d,
     UNNEST(d.accessorials) a
LEFT JOIN `144240581301.freight_gold.load_reference` r ON r.load_id = d.load_id;


-- ---------------------------------------------------------------------
-- 7) References — PO / pickup / delivery / appointment / trailer / container / seal
--    numbers, which `order_number` alone used to absorb indiscriminately.
-- ---------------------------------------------------------------------
-- DEDUPED. On the 500-packet run this was a catch-all sink: 33% of 3,118 rows came back
-- as ref_type 'other' (one BOL contributed 28 references, 26 of them 'other'), and 173
-- rows were the same value recorded twice on the same document. SELECT DISTINCT kills the
-- duplicates; is_identified separates the references that actually mean something from
-- the page-noise, without discarding either.
CREATE OR REPLACE TABLE `144240581301.freight_gold.doc_references` AS
SELECT DISTINCT
  d.load_id,
  r.packet_load_number,
  d.doc_class, d.page_range,
  ref.ref_type,
  ref.value,
  (ref.ref_type != 'other') AS is_identified,
  d.needs_review
FROM `144240581301.freight_gold.documents_classified` d,
     UNNEST(d.references) ref
LEFT JOIN `144240581301.freight_gold.load_reference` r ON r.load_id = d.load_id;


-- ---------------------------------------------------------------------
-- Sanity checks (run as needed):
--   -- POD coverage — the number this whole schema change exists for. Was 14%.
--   SELECT COUNT(DISTINCT load_id) AS loads_with_pod
--   FROM `144240581301.freight_gold.proofs_of_delivery`;
--   SELECT source_doc_class, COUNT(*) FROM `144240581301.freight_gold.proofs_of_delivery`
--   GROUP BY 1;   -- expect most PODs to be source_doc_class = 'bill_of_lading'
--
--   -- multi-stop loads, previously invisible:
--   SELECT load_id, COUNT(*) n FROM `144240581301.freight_gold.stops`
--   GROUP BY 1 HAVING n > 2 ORDER BY n DESC;
--
--   -- accessorial revenue by type:
--   SELECT accessorial_type, COUNT(*) n, SUM(amount) total
--   FROM `144240581301.freight_gold.accessorials` GROUP BY 1 ORDER BY total DESC;
--
--   -- rate per mile distribution (the headline ML feature):
--   SELECT APPROX_QUANTILES(rate_per_mile, 10) FROM `144240581301.freight_gold.rate_confirmations`
--   WHERE rate_per_mile IS NOT NULL;
--
--   -- did carrier_key actually collapse the DB7 spellings?
--   SELECT carrier_key, COUNT(DISTINCT carrier_name) spellings, COUNT(*) rows
--   FROM `144240581301.freight_gold.documents_classified`
--   GROUP BY 1 ORDER BY rows DESC LIMIT 20;
--
-- More checks:
--   -- doc mix:
--   SELECT doc_class, COUNT(*) FROM `144240581301.freight_gold.documents_classified` GROUP BY 1 ORDER BY 2 DESC;
--
--   -- row counts per gold table:
--   SELECT 'rate_confirmations' t, COUNT(*) n FROM `144240581301.freight_gold.rate_confirmations`
--   UNION ALL SELECT 'bills_of_lading',    COUNT(*) FROM `144240581301.freight_gold.bills_of_lading`
--   UNION ALL SELECT 'proofs_of_delivery', COUNT(*) FROM `144240581301.freight_gold.proofs_of_delivery`
--   UNION ALL SELECT 'other_documents',    COUNT(*) FROM `144240581301.freight_gold.other_documents`;
--
--   -- CROSS-CHECK the three tables (JOIN ON load_id — see the header):
--   SELECT rc.load_id, rc.packet_load_number,
--          rc.carrier_key   AS rc_carrier,  b.carrier_key   AS bol_carrier,
--          rc.weight        AS rc_weight,   b.weight        AS bol_weight,
--          rc.delivery_date AS rc_delivery, p.delivery_date AS pod_delivery, p.delivered
--   FROM       `144240581301.freight_gold.rate_confirmations` rc
--   LEFT JOIN  `144240581301.freight_gold.bills_of_lading`    b USING (load_id)
--   LEFT JOIN  `144240581301.freight_gold.proofs_of_delivery` p USING (load_id)
--   WHERE rc.carrier_key != b.carrier_key OR rc.weight != b.weight;   -- mismatches to review
--
--   -- packets whose documents disagree on the load number (the old silent-join failure):
--   SELECT load_id, packet_load_number FROM `144240581301.freight_gold.load_reference`
--   WHERE load_number_conflict;
--
--   -- rate cons whose money breakdown does not add up (line_haul is unusable on these):
--   SELECT load_id, packet_load_number, rate_total, line_haul, fuel_surcharge
--   FROM `144240581301.freight_gold.rate_confirmations`
--   WHERE rate_breakdown_conflict ORDER BY rate_total DESC;
--
--   -- how much the pallet clamp caught (raw kept beside the clamped value):
--   SELECT load_id, doc_class, pallet_count_raw, pallet_count
--   FROM `144240581301.freight_gold.documents_classified`
--   WHERE pallet_count_raw IS NOT NULL AND pallet_count IS NULL ORDER BY pallet_count_raw DESC;
--
--   -- documents whose dates need a human look:
--   SELECT load_id, doc_class, pickup_date, delivery_date
--   FROM `144240581301.freight_gold.documents_classified` WHERE date_suspect;
-- ---------------------------------------------------------------------
