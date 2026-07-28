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
      │     2. Gemini reads the IMAGE + the OCR TEXT → structured JSON
      │        (image for layout/classification, OCR text for exact numbers/IDs)
      ▼
Cloud Storage      gs://<bucket>/json/<load_id>.json     ← lossless source of truth
      │   bq load  +  SQL transform
      ▼
BigQuery      freight.documents   (1 row per document found)
              freight.loads       (1 row per load, fields rolled up)
      │   export
      ▼
Parquet / CSV for ML training      +   (later) a Looker Studio dashboard
```

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

The docs are **scanned** (images, not text) and every broker's layout differs, so a template approach breaks. We use each tool for what it's best at:

- **Document AI OCR** does the *reading* — strong on degraded scans and handwriting, and it returns **word-level confidence**, a trustworthy signal (unlike an LLM's self-reported confidence).
- **Gemini** does the *understanding* — it reads the page image **plus** the OCR text, classifies each document in the packet, finds page ranges, and extracts the fields. Feeding it the OCR text anchors exact numbers/IDs and cuts hallucinated digits.

There is no pre-built "Bill of Lading" or "Rate Confirmation" processor in Document AI (they're trucking-specific), so Gemini defines and fills the fields.

## The data model

| Table | One row per… | Key columns |
|---|---|---|
| `freight.documents` | document found inside a packet | `load_id`, `doc_type`, `page_range`, `confidence` (Gemini), `ocr_confidence` (Document AI), `needs_review`, + all extracted fields |
| `freight.loads` | load (folder) | `load_id`, rolled-up broker/carrier/lane/dates/rate, `doc_count`, `docs_needing_review` |

`load_id` = the **Drive folder name**, so linking documents to loads is automatic — the folder *is* the load.

## Reliability (why it won't die overnight and lose progress)

- **Decoupled steps** — the Drive copy and the extraction are separate jobs. Extraction re-runs against a frozen Cloud Storage snapshot; Drive is never in the hot path.
- **Pre-flight PDF validation** — each PDF is opened with PyMuPDF first; empty/corrupt/encrypted files go to `logs/invalid/` and are skipped before any paid call.
- **Resumable / idempotent** — each PDF's output is a file named by `load_id`; a restart skips anything already done. A crash costs minutes, never the whole batch.
- **Per-document error isolation** — one bad PDF is logged to `logs/failed/` and skipped; it can't stop the run.
- **Retries only on transient errors** — network hiccups, rate limits, and 5xx from Gemini/Document AI auto-retry with backoff; malformed PDFs, permission-denied, and schema failures fail fast (no wasted calls).
- **`needs_review` flag** — trips when Gemini confidence *or* Document AI OCR confidence is low (or the model marks a field illegible). On degraded scans the model is told to return `null` rather than guess.

## Cost & time (one-time)

- **Reading + extraction:** Document AI OCR (~$1.50 / 1,000 pages) + Gemini Flash — realistically tens of dollars total; the pilot gives the exact per-PDF cost × 19,000.
- **Storage:** a few dollars/month for PDFs + JSON.
- **Runtime:** a few hours; resumable, so duration isn't a risk.

## What's in this project

```
README.md                      ← you are here (architecture)
BUILD_GUIDE.md                 ← step-by-step, assumes zero cloud knowledge
DECISIONS.md                   ← decisions, watch-list, cost savings
requirements.txt               ← Python libraries to install
schema/
  extraction_schema.json       ← the fields we pull (human-readable spec — EDIT THIS)
  bigquery_setup.sql           ← creates the BigQuery tables + loads the data
pipeline/
  extract.py                   ← the extraction script (edit the config values, run)
```

Start with **BUILD_GUIDE.md**.
