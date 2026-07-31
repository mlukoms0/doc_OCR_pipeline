# Freight Document Extraction — Architecture

A **one-time** project to turn ~19,000 historical, scanned freight PDFs into clean, structured data for machine learning. Live/future documents are handled separately by the company's own app — this pipeline only processes the existing backlog.

## The whole pipeline in one picture

```
Google Drive  (19,000 load folders — one multi-page scanned PDF each)
      │   one-time copy with rclone (resumable, verified)
      ▼
Cloud Storage      gs://<bucket>/raw/loads/<load_id>/<file>.pdf
      │   extract.py, per PDF:
      │     1. Document AI OCR  → reliable text + per-page confidence
      │     2. pages rendered locally to 150-DPI JPEGs (cheapest input tokens)
      │     3. Gemini reads the IMAGES + the PAGE-MARKED OCR TEXT → structured JSON
      ▼
Cloud Storage      gs://<bucket>/json/<load_id>.json     ← lossless source of truth
      │   bq load --replace  +  bigquery_setup.sql (Section B)
      ▼
BigQuery (silver)  freight.documents        (1 row per document found)
      │   gold_layer.sql
      ▼
BigQuery (gold)    freight_gold.rate_confirmations   ┐
                   freight_gold.bills_of_lading       ├─ join on load_id
                   freight_gold.proofs_of_delivery    ┘   to reconcile a load
      │   export
      ▼
Parquet / CSV for ML   +   (later) a Looker Studio dashboard
```

## The Rate Confirmation is the anchor

A freight packet holds a rate confirmation (RC), a bill of lading (BOL), sometimes a proof of delivery (POD), plus misc docs. **The RC is the authoritative source** — a standardized, broker-generated document with clean fields; BOLs are shipper-generated, often messy or handwritten. So the model **trusts the RC first** and uses the BOL/POD to fill gaps and cross-check.

Each document is extracted **from its own pages only** — the model never copies a value from one document onto another. Reconciliation happens later, in the gold layer, where it's visible and auditable.

Two identifiers, kept **separate**:
- **`load_number`** — the load's reference number, as printed on a given document. A *cross-check*, not a key (see below).
- **`rc_number`** — the rate confirmation's **own** document number. Often the same single number as `load_number`; distinct only when the RC prints two.

## Why these tools (plain English)

| Tool | What it is | Why we use it |
|---|---|---|
| **Google Drive** | Where the PDFs live today | The source — copied *out* once; not built for a big batch job |
| **rclone** | A free file-copy utility | Copies Drive → Cloud Storage reliably, with retries + resume |
| **Cloud Storage (GCS)** | Google's "hard drive in the cloud" | Durable, cheap, native input for Google's AI tools |
| **Document AI (OCR)** | Google's dedicated document-reading engine | Best-in-class OCR of scans + handwriting; gives per-word **confidence** we can trust |
| **Gemini (via Vertex AI)** | Multimodal AI that sees images + returns structured data | Classifies each doc, splits page ranges, extracts fields across messy layouts |
| **BigQuery** | Google's cloud database for analytics | Where clean, queryable data lands; feeds ML export + the future dashboard |

## Why Document AI + Gemini together

The docs are **scanned** (images, not text) and every broker's layout differs, so a template approach breaks. Each tool does what it's best at:
- **Document AI OCR** does the *reading* — strong on degraded scans/handwriting, and returns **word-level confidence** we can trust (unlike an LLM's self-report).
- **Gemini** does the *understanding* — reads the page image **plus** the OCR text, classifies each document, finds page ranges, extracts fields. The OCR text anchors exact numbers/IDs and cuts hallucinated digits.

## The data model

**Silver — `freight.documents`** (one row per document): every extracted field + `doc_type`, `page_range`, `confidence` (Gemini), `ocr_confidence` (Document AI), `unverified_fields` (cross-check), `needs_review`.

**Gold — three per-type tables** (built by `gold_layer.sql`), each one row per document, all starting with `load_id, packet_load_number, load_number, source_pdf, page_range` so you **join them on `load_id`** to reconcile a load across its documents:

| Table | One row per… | Highlights |
|---|---|---|
| `freight_gold.rate_confirmations` | rate con | `rc_number`, `rate_total` + `rate_breakdown_conflict`, `broker_name`, carrier, lane, dates, pallets, commodity, weight |
| `freight_gold.bills_of_lading` | BOL | `bol_number`, shipper/consignee, weight, pieces, freight class |
| `freight_gold.proofs_of_delivery` | POD | delivered, received_by, signature, delivery_date |

(`freight_gold.other_documents` keeps packing slips / lumper / invoices so nothing is dropped. `documents_classified` normalizes `doc_type`, adds `carrier_key`, clamps impossible pallet counts and flags impossible dates. `load_reference` resolves one `packet_load_number` per load.)

### Join on `load_id`, not `load_number`

`load_id` is the packet folder — one folder = one packet = one load — and it is present on **100%** of rows. `load_number` is a number read off a scan. Measured on the 1,000-load run:

| | join on `load_number` | join on `load_id` |
|---|---|---|
| rate cons matched to their BOL | 869 / 1209 (**72%**) | 1188 / 1209 (**98%**) |
| rows joined to a *different* load's BOL | **6** | 0 |

`load_number` is NULL on 25.6% of BOLs, and three number pairs collide across unrelated folders. So it's carried as a **cross-check** (`load_number` per document, `load_number_conflict` when a packet's documents disagree) while `packet_load_number` — resolved once per packet from the rate con — is the number you report and track.

**Reconcile a load — does the BOL/POD agree with the RC?**
```sql
SELECT rc.load_id, rc.packet_load_number,
       rc.carrier_name AS rc_carrier, b.carrier_name AS bol_carrier,
       rc.weight AS rc_weight,        b.weight AS bol_weight,
       p.delivered
FROM       `freight_gold.rate_confirmations` rc
LEFT JOIN  `freight_gold.bills_of_lading`    b USING (load_id)
LEFT JOIN  `freight_gold.proofs_of_delivery` p USING (load_id);
```

## Trust hierarchy (why we don't rely on any one signal)

1. **Gemini `confidence`** — the model rating itself. Weak/noisy; ~73% pinned at 1.0 on the pilot. Don't trust alone.
2. **`ocr_confidence`** — a *real* per-token score from Document AI's OCR model. Trustworthy for **legibility**.
3. **`unverified_fields`** — a deterministic, **page-scoped** cross-check: does each key value (rate, weight, IDs, `rc_number`) actually appear in the OCR text of that document's pages? Catches hallucinated values. Zero cost.
   *Know its range:* strong on **long** values (7+ digit IDs, 4+ digit money) — a flag there is real. Weak on **short** numbers, which match almost any page by coincidence. `pallet_count` was dropped from it for that reason (a real count is 1–2 characters, so it was never actually checked); pallet plausibility is enforced in the gold layer instead.

`needs_review` trips when: Gemini `confidence < 0.85`, **or** `ocr_confidence < 0.92`, **or** the doc is illegible, **or** `unverified_fields` is non-empty.

Because extraction no longer copies values between documents, a flag now means what it says. Previously the model stamped the RC's load number onto the BOL, the page-scoped check correctly reported it wasn't printed there, and that single benign pattern generated **48% of the entire BOL review queue**.

## Reliability (why it won't die overnight and lose progress)

- **Decoupled steps** — Drive copy and extraction are separate; extraction re-runs against a frozen GCS snapshot.
- **Pre-flight PDF validation** (PyMuPDF) — empty/corrupt/encrypted files go to `logs/invalid/` before any paid call.
- **Resumable / idempotent** — each PDF's output is keyed by `load_id`; a restart skips what's done. A crash costs minutes.
- **Multi-PDF folders** — recorded to `pipeline/logs/multidocs.csv` + a GCS manifest and **skipped** (multi-doc support later); they don't stop the run.
- **Per-document error isolation** — one bad PDF → `logs/failed/`, run continues.
- **Retries only on transient errors** — network/429/5xx auto-retry with backoff; bad PDFs / permission / schema failures fail fast.
- **OCR and Gemini retry independently** — a Gemini 429 no longer re-runs Document AI. They used to share one decorator, so the exact failure `MAX_WORKERS` provokes re-paid for OCR up to 5 times.
- **Loading is idempotent** — `bq load --replace` rebuilds staging from the whole `json/` prefix. Without `--replace` bq *appends*, and since extraction is resumable you load more than once, silently doubling every row through to gold.

## Cost & time (one-time)

- **Reading + extraction:** Document AI OCR (~$1.50 / 1,000 pages) + Gemini Flash — tens of dollars total. `extract.py` prints an **exact usage tally** (pages, tokens, $) at the end of every run + a 19k projection. The tally counts **thinking tokens** separately; they bill at the output rate and are not part of `candidates_token_count`, so leaving them out under-reported the run.
- **Input cost:** pages are rendered locally to `RENDER_DPI` (150) JPEGs rather than shipping the PDF. Gemini bills images per 768px tile, so halving the DPI quarters the tiles. Exact digits come from the OCR text, so the image only has to carry layout.
- **Full-run speed:** `extract.py` runs `MAX_WORKERS` PDFs concurrently (default **12**; a `ThreadPoolExecutor`, the work is I/O-bound) — ~20–30 hours of sequential work finishes in tens of minutes. Lower `MAX_WORKERS` if you hit `429`s.
- **Storage:** pennies.

## What's in this project

```
README.md, BUILD_GUIDE.md, DECISIONS.md
requirements.txt
schema/
  extraction_schema.json     ← the field spec (human-readable)
  bigquery_setup.sql         ← silver: staging + documents (+ flatten INSERT)
  gold_layer.sql             ← gold: the 3 per-type tables (join on load_number)
pipeline/
  extract.py                 ← the extraction script (edit the config values, run)
```

Start with **BUILD_GUIDE.md**.
