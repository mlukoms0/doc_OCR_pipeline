

## pipeline

```
Google Drive  (19,000 load folders in this case)
      │   one-time copy with rclon
      ▼
Cloud Storage      gs://<bucket>/raw/loads/<load_id>/<file>.pdf
      │   extract.py, per PDF:
      │     1. Document AI OCR  → reliable text + per-page confidence
      │     2. pages rendered locally to 150-DPI JPEGs
      │     3. Gemini reads the IMAGES + the PAGE-MARKED OCR TEXT → structured JSON
      ▼
Cloud Storage      gs://<bucket>/json/<load_id>.json    
      │            Bronze layer flat table
      ▼
BigQuery (silver)   flat load and docuemnts table
      │  
      ▼
BigQuery (gold)    documents classified view, per doc type tables
                             
      │   export
      ▼
Parquet / CSV for ML  
```

## Doc type notes.

A freight packet holds a rate confirmation (RC), a bill of lading (BOL), sometimes a proof of delivery (POD), plus misc. docs. — RC is main document for the extracotr.

Each document is extracted from its own pages only — the model never copies a value from one document onto another. Reconciliation happens in Gold layer




##tools 

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

**Gold — three per-type tables** (built by `gold_layer.sql`), each one row per document, all starting with `load_id, packet_load_number, load_number, source_pdf, page_range` Joined on load_id.

| Table | One row per… | Highlights |
|---|---|---|
| `freight_gold.rate_confirmations` | rate con | `rc_number`, `rate_total` + `rate_breakdown_conflict`, `broker_name`, carrier, lane, dates, pallets, commodity, weight |
| `freight_gold.bills_of_lading` | BOL | `bol_number`, shipper/consignee, weight, pieces, freight class |
| `freight_gold.proofs_of_delivery` | POD | delivered, received_by, signature, delivery_date |

(`freight_gold.other_documents` keeps packing slips / lumper / invoices `documents_classified` normalizes `doc_type`, adds `carrier_key`, identifies impossible pallet counts and flags impossible dates. `load_reference` resolves one `packet_load_number` per load.)



## Reliability 
- **Pre-flight PDF validation** (PyMuPDF) — empty/corrupt/encrypted files go to `logs/invalid/` before any paid call.
- **Resumable / idempotent** — each PDF's output is keyed by `load_id`; a restart skips what's done.
- **Per-document error isolation** — one bad PDF → `logs/failed/`, run continues.
- **Retries only on transient errors** — network/429/5xx auto-retry with backoff; bad PDFs / permission / schema failures fail fast.
- **OCR and Gemini retry independently**
- **Loading is idempotent** — `bq load --replace` rebuilds staging from the whole `json/` prefix. 

## Cost & time 

- **Reading + extraction:** Document AI OCR (~$1.50 / 1,000 pages) + Gemini Flash — tens of dollars total. `extract.py` prints an **exact usage tally** (pages, tokens, $) at the end of every run + a 19k projection. The tally counts **thinking tokens** separately; they bill at the output rate and are not part of `candidates_token_count`, so leaving them out under-reported the run.
- **Input cost:** pages are rendered locally to `RENDER_DPI` (150) JPEGs rather than shipping the PDF. Gemini bills images per 768px tile, so halving the DPI quarters the tiles. Exact digits come from the OCR text, so the image only has to carry layout.
- **Full-run speed:** `extract.py` runs `MAX_WORKERS` PDFs concurrently (default **12**; a `ThreadPoolExecutor`, the work is I/O-bound) — ~20–30 hours of sequential work finishes in tens of minutes. Lower `MAX_WORKERS` if you hit `429`s.
- **Storage:** pennies.


