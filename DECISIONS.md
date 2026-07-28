# Decisions & Watch-list

_One-time backfill: ~19k scanned freight PDFs (Google Drive) → structured data for ML. Last updated 2026-07-28._

## Final decisions

- **Scope:** backfill only. Future/live docs handled by the company app — no ongoing pipeline built here.
- **Flow:** Drive → `rclone` copy → Cloud Storage → **Document AI OCR → Gemini** (per PDF) → JSON in GCS → **BigQuery**.
- **Engine = Document AI OCR + Gemini (hybrid).** Document AI does the reading (strong on scans/handwriting, trustworthy word-level confidence); Gemini reads the image **+** the OCR text to classify, split page ranges, and extract fields. No pre-built BOL/rate-con processor exists, so Gemini defines the fields.
- **Unit of work = the load.** Drive folder name = `load_id`; one packet PDF per folder. Linking docs→loads is automatic.
- **Reliability:** pre-flight PDF validation (PyMuPDF), resumable + idempotent (output keyed by `load_id`), per-doc error isolation, **retry only transient errors** (network / 429 / 5xx), `needs_review` on low Gemini *or* OCR confidence.
- **Store:** raw JSON in GCS (lossless source of truth) + 2 BigQuery tables. Dashboard (Looker Studio) is phase 2.

## Schema (summary — full spec in `schema/`)

- **`extraction_schema.json`** — ~40 fields per document; doc types: `rate_confirmation`, `bill_of_lading`, `proof_of_delivery`, `invoice`, `lumper_receipt`, `packing_list`, `other`.
- **BigQuery** (`schema/bigquery_setup.sql`):
  - `freight.documents` — 1 row per document (ids, parties, lane, dates, money, freight, POD fields, `confidence`, `ocr_confidence`, `needs_review`).
  - `freight.loads` — 1 row per load, fields rolled up + `doc_count`, `docs_needing_review`.
- `ocr_confidence` is added by the pipeline (from Document AI), not by Gemini.
- Editing fields: change all three in sync — `extraction_schema.json`, the Pydantic model in `pipeline/extract.py`, and the SQL.

## Things to worry about

- **Confidently-wrong numbers** on faded scans (rate/weight/MC#). Mitigated by feeding OCR text to Gemini + "return null, don't guess" + `needs_review`, but **spot-check the pilot**.
- **Handwriting (PODs)** = lower accuracy; expect a higher review rate there.
- **Multi-PDF folders:** `validate_load_folders()` hard-stops the run if any folder ≠ 1 PDF. Fix the flagged folders (or relax it to warn-and-skip) before a big run.
- **Folder names as `load_id`:** must be unique/clean or IDs collide.
- **Date/number formats** vary — prompt normalizes to `YYYY-MM-DD` / plain numbers; validate after.
- **Two regions in play:** Vertex/Gemini = `us-east1`; Document AI = `us` (its US multi-region). Expected — keep the bucket + BigQuery in US too.
- **Quotas:** Vertex and Document AI both have per-minute caps; heavy 429s → throttle or switch to batch.
- **Sensitive data:** BOLs contain names/addresses — keep the bucket private (default), don't share exports loosely.
- **Model drift:** pin the model name; note which model produced the data.

## Cost-saving measures

- **Pilot first (100 docs)** — never process 19k blind.
- **Gemini Flash**, not Pro.
- **One OCR + one Gemini call per whole packet**, not per page.
- **Downscale scans (~150 DPI)** before sending — biggest lever on Gemini input-token cost.
- **`temperature=0` + tight prompt + structured output** — fewer wasted tokens.
- **Retry only transient errors** — no burning 5 calls on a bad PDF or a 403.
- **Resumable** — never re-pay for completed docs.
- **Vertex batch prediction for the full run** — ~50% cheaper than online calls.
- **Storage:** after extraction, move `raw/loads/` PDFs to a colder class (Nearline/Coldline) or delete (Drive is the backup); auto-delete temp files with a lifecycle rule.
- **Document AI OCR** is cheap (~$1.50/1k pages) but not free — the main added cost vs Gemini-only; worth it for reading quality + trustworthy confidence.
- **BigQuery** at this size is effectively free-tier.
