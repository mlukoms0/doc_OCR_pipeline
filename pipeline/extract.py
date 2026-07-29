"""
Freight packet extraction — Document AI OCR + Gemini, one pass per PDF.

Per PDF:
  - Validate the PDF opens and has pages (skip broken files)
  - Document AI OCR -> reliable text layer + per-page confidence
  - Gemini reads the IMAGE + the OCR TEXT together -> structured JSON
    (image for layout/classification, OCR text for exact numbers/IDs)
  - Attach Document AI's confidence to each extracted document
  - Write gs://<BUCKET>/<JSON_PREFIX>/<load_id>.json  (resumable)

Run the PILOT first (LIMIT = 100), review in BigQuery, then set LIMIT = None.
"""

import csv
import json
import re
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Literal, Optional

import fitz  # PyMuPDF — validates PDFs before they reach the models
import httpx
from google import genai
from google.genai import types
from google.genai import errors as genai_errors
from google.cloud import storage
from google.cloud import documentai
from google.api_core import exceptions as gapi_exceptions
from pydantic import BaseModel
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

# ============ EDIT THESE VALUES ============
PROJECT_ID       = "144240581301"
LOCATION         = "us-east1"          # Vertex/Gemini region (keep in sync with bucket)
BUCKET           = "doc-archive67"
LIMIT            = 1000                  # PILOT: 100. Full run: set to None
MAX_WORKERS      = 12                   # concurrent PDFs; lower this if you hit 429 rate limits
NEWEST_FIRST     = True                 # True = highest load number (newest) first; False = oldest first
DOCAI_LOCATION   = "us"                 # Document AI multi-region: "us" or "eu"
OCR_PROCESSOR_ID = "a889dd87be6492b0"   # from the Document OCR processor you create
# ===========================================

PDF_PREFIX  = "testing_set_raw/"
JSON_PREFIX = "json/"
MODEL       = "gemini-2.5-flash"

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET)
docai_client = documentai.DocumentProcessorServiceClient(
    client_options={"api_endpoint": f"{DOCAI_LOCATION}-documentai.googleapis.com"}
)

# ---- Usage tally (exact counts) + rough cost estimate ----
# The token/page COUNTS are exact. The $ rates below are APPROXIMATE — verify on the
# current Vertex AI + Document AI pricing pages and edit to match.
GEMINI_INPUT_PER_1M    = 0.30    # Gemini 2.5 Flash, per 1M input tokens (USD, approx)
GEMINI_OUTPUT_PER_1M   = 2.50    # per 1M output tokens (USD, approx)
DOCAI_OCR_PER_1K_PAGES = 1.50    # Document AI OCR, per 1,000 pages (USD, approx)
USAGE = {"ocr_pages": 0, "in_tokens": 0, "out_tokens": 0}
_usage_lock = threading.Lock()   # USAGE is mutated from worker threads — guard the increments


# ---- The schema Gemini must return (mirror of schema/extraction_schema.json) ----
# NOTE: ocr_confidence is NOT here — the pipeline adds it after extraction, from
# Document AI, so Gemini is never asked to invent it.
class Document(BaseModel):
    # Literal (not str) so the response schema actually CONSTRAINS Gemini to these values.
    # As a plain `str` it returned 21 different spellings; this pins it to the enum.
    doc_type: Literal[
        "rate_confirmation", "bill_of_lading", "proof_of_delivery",
        "invoice", "lumper_receipt", "packing_list", "other",
    ]
    page_range: str
    confidence: float
    illegible: bool = False

    load_number: Optional[str] = None
    pro_number: Optional[str] = None
    bol_number: Optional[str] = None
    order_number: Optional[str] = None
    rc_number: Optional[str] = None

    broker_name: Optional[str] = None
    broker_mc: Optional[str] = None
    carrier_name: Optional[str] = None
    carrier_mc: Optional[str] = None
    carrier_dot: Optional[str] = None
    carrier_scac: Optional[str] = None

    shipper_name: Optional[str] = None
    shipper_address: Optional[str] = None
    consignee_name: Optional[str] = None
    consignee_address: Optional[str] = None

    origin_city: Optional[str] = None
    origin_state: Optional[str] = None
    origin_zip: Optional[str] = None
    destination_city: Optional[str] = None
    destination_state: Optional[str] = None
    destination_zip: Optional[str] = None

    pickup_date: Optional[str] = None      # YYYY-MM-DD
    delivery_date: Optional[str] = None

    rate_total: Optional[float] = None
    line_haul: Optional[float] = None
    fuel_surcharge: Optional[float] = None
    currency: Optional[str] = None

    commodity: Optional[str] = None
    weight: Optional[float] = None
    weight_unit: Optional[str] = None
    pieces: Optional[int] = None
    pallet_count: Optional[int] = None
    pallet_spaces: Optional[int] = None
    equipment_type: Optional[str] = None
    freight_class: Optional[str] = None
    hazmat: Optional[bool] = None

    delivered: Optional[bool] = None
    received_by: Optional[str] = None
    signature_present: Optional[bool] = None
    exceptions_notes: Optional[str] = None


class Packet(BaseModel):
    packet_summary: str
    documents: List[Document]


PROMPT = """You are extracting data from a SCANNED freight load packet: a single PDF
of 2-5 pages that may contain a rate confirmation, a bill of lading (BOL), a proof of
delivery (POD), an invoice, and/or a lumper receipt.

You are given BOTH the page images AND OCR text extracted from the same PDF by a
dedicated OCR engine. Use the OCR text to read exact numbers, IDs, and codes precisely;
use the images for layout, document boundaries, classification, and page ranges. If they
conflict, prefer whichever is clearly correct.

Rules:
- Identify EACH distinct document in the packet: its type, and the page range it covers.
- Extract the fields for each document into the schema.
- Use null for anything not present. If a value is present but you cannot read it clearly,
  use null and do NOT guess — especially numbers (rates, weights, MC/DOT numbers).
- If any field on a document was hard to read, set "illegible": true and lower "confidence".
- doc_type MUST be one of the allowed enum values. A rate confirmation is sometimes titled
  "Load Confirmation", "Carrier Confirmation", or "Load Sheet" — classify all of those as
  rate_confirmation. Anything that fits none of the values is "other".
- carrier_name is the trucking COMPANY name, never a dispatcher or agent person's name.
- Dates must be YYYY-MM-DD. Money and weights must be plain numbers (no $ or commas);
  weight is a whole number of pounds unless the document clearly prints a fraction.
- The RATE CONFIRMATION is the authoritative document for a load — when a value differs
  between documents, trust the rate confirmation. From rate confirmations, capture:
  * load_number — the shared reference that also appears on the BOL and POD.
  * rc_number — the rate confirmation's OWN document number, distinct from load_number.
    load_number is shared across documents; rc_number identifies the confirmation itself
    and appears only on rate confirmations. Do not copy load_number into rc_number.
  * rate_total, line_haul, and fuel_surcharge.
  * total weight.
  * pallet_count (number of physical pallets) and pallet_spaces (pallet positions/spaces
    used in the trailer) — look for labels like PLT, PLTS, SPACE, SPCs, or "positions".
  * the FULL commodity description.
  * broker_name.
Return only JSON matching the schema."""


def load_id_from_pdf(blob_name: str) -> str:
    """<PDF_PREFIX>/<load_id>/<file>.pdf  ->  <load_id>"""
    return blob_name[len(PDF_PREFIX):].split("/")[0]


def _load_num(blob_name: str) -> int:
    """Numeric sort key for a load id (the folder name). Non-numeric ids sort last."""
    lid = load_id_from_pdf(blob_name)
    return int(lid) if lid.isdigit() else -1


def find_multi_pdf_loads(pdf_blobs) -> set:
    """Group the given PDF blobs by load. Loads with exactly one PDF are processed; loads
    with more than one are RECORDED (multidocs.csv + GCS manifest) and skipped for now
    (multi-doc support comes later). Never raises. Returns the set of load_ids to skip."""
    loads = defaultdict(list)
    for blob in pdf_blobs:
        loads[load_id_from_pdf(blob.name)].append(blob.name)

    multi = {lid: files for lid, files in loads.items() if len(files) > 1}

    if multi:
        # Local CSV summary in pipeline/logs/: folder, doc_count
        logs_dir = Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        csv_path = logs_dir / "multidocs.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["folder", "doc_count"])
            for lid, files in sorted(multi.items()):
                writer.writerow([lid, len(files)])
        # Detailed manifest (with filenames) to GCS — useful for the later multi-doc work
        manifest = []
        for lid, files in sorted(multi.items()):
            manifest.append(f"{lid} ({len(files)} PDFs)")
            manifest.extend(f"    {f}" for f in files)
        bucket.blob("logs/multi_pdf_loads.txt").upload_from_string("\n".join(manifest))
        print(f"⚠️  {len(multi)} of {len(loads)} loads have multiple PDFs — skipping them "
              f"for now (multi-doc support later).")
        print(f"    Wrote {csv_path} + gs://{BUCKET}/logs/multi_pdf_loads.txt")
    else:
        print(f"✅ All {len(loads)} loads have exactly one PDF.")

    return set(multi)


def validate_pdf(pdf_bytes: bytes) -> None:
    """Raise ValueError if the PDF is empty, unreadable, encrypted, or has 0 pages.
    Structural check only — a valid-but-blurry scan still passes and is flagged later
    by the OCR/Gemini confidence + needs_review."""
    if not pdf_bytes:
        raise ValueError("empty file (0 bytes)")
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:                       # fitz raises on truncated / non-PDF data
        raise ValueError(f"cannot open as PDF: {e}")
    try:
        if doc.needs_pass:
            raise ValueError("password-protected PDF")
        if doc.page_count == 0:
            raise ValueError("PDF has 0 pages")
    finally:
        doc.close()


def ocr_document(pdf_bytes: bytes):
    """Run Document AI OCR. Returns (full_text, per_page_text, per_page_mean_confidence).
    per_page_text lets the cross-check verify a document against ONLY its own pages."""
    name = docai_client.processor_path(PROJECT_ID, DOCAI_LOCATION, OCR_PROCESSOR_ID)
    result = docai_client.process_document(
        request=documentai.ProcessRequest(
            name=name,
            raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf"),
        )
    )
    doc = result.document
    full_text = doc.text or ""
    with _usage_lock:
        USAGE["ocr_pages"] += len(doc.pages)    # every page here is a billable OCR page
    page_conf, page_text = [], []
    for page in doc.pages:
        confs = [t.layout.confidence for t in page.tokens if t.layout and t.layout.confidence]
        page_conf.append(round(sum(confs) / len(confs), 3) if confs else 0.0)
        # slice this page's own text out of the full document text (for page-scoped checks)
        segs = page.layout.text_anchor.text_segments if page.layout and page.layout.text_anchor else []
        page_text.append("".join(full_text[int(s.start_index):int(s.end_index)] for s in segs))
    return full_text, page_text, page_conf


def pages_from_range(page_range: str):
    """'1-2' -> [1, 2];  '3' -> [3];  '1,3' -> [1, 3]."""
    pages = []
    for part in str(page_range).split(","):
        part = part.strip()
        if "-" in part:
            try:
                a, b = part.split("-")
                pages.extend(range(int(a), int(b) + 1))
            except ValueError:
                continue
        elif part.isdigit():
            pages.append(int(part))
    return pages


def ocr_confidence_for(page_range: str, page_conf) -> Optional[float]:
    """Mean Document AI confidence over the pages this document spans."""
    if not page_conf:
        return None
    vals = [page_conf[p - 1] for p in pages_from_range(page_range) if 1 <= p <= len(page_conf)]
    if not vals:
        vals = page_conf  # fallback: whole-packet mean
    return round(sum(vals) / len(vals), 3)


def page_text_for(page_range: str, page_text) -> str:
    """OCR text of ONLY the pages this document spans (falls back to all pages)."""
    if not page_text:
        return ""
    parts = [page_text[p - 1] for p in pages_from_range(page_range) if 1 <= p <= len(page_text)]
    return " ".join(parts) if parts else " ".join(page_text)


# Fields worth cross-checking against the OCR text — the hallucination-prone ones
# (money + identifiers). Dates/names are skipped: Gemini reformats them, so a literal
# match would false-alarm.
KEY_FIELDS = [
    "load_number", "pro_number", "bol_number", "order_number", "rc_number",
    "broker_mc", "carrier_mc", "carrier_dot", "carrier_scac",
    "rate_total", "line_haul", "fuel_surcharge", "weight", "pallet_count",
]

_NON_ALNUM = re.compile(r"[^a-z0-9]")


def _norm(value) -> str:
    """Lowercase, keep only letters/digits (so '$1,850.00' -> '185000')."""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return _NON_ALNUM.sub("", str(value).lower())


def verify_document(doc_dict: dict, norm_text: str) -> str:
    """Deterministic cross-check (NO AI, NO cost): which KEY_FIELDS hold a value that
    does NOT appear in the OCR text of THIS document's own pages? Catches hallucinated
    values. `norm_text` is the already-normalized text for the doc's page range.
    Returns a comma-joined list of field names ('' if all verified)."""
    unverified = []
    for field in KEY_FIELDS:
        value = doc_dict.get(field)
        if value in (None, "", []):
            continue
        norm_value = _norm(value)
        if len(norm_value) < 3:
            continue                       # too short to verify without false alarms
        if norm_value not in norm_text:
            unverified.append(field)
    return ",".join(unverified)


def is_transient(exc: BaseException) -> bool:
    """Retry ONLY errors that could succeed next time: network hiccups, rate limits,
    and 5xx from either Gemini or Document AI. Malformed PDFs, permission denied (403),
    invalid argument, and schema/JSON failures fail immediately."""
    if isinstance(exc, (TimeoutError, ConnectionError,
                        httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, genai_errors.ServerError):                       # Gemini 5xx
        return True
    if isinstance(exc, genai_errors.ClientError) and getattr(exc, "code", None) == 429:
        return True                                                     # Gemini rate limit
    if isinstance(exc, (gapi_exceptions.ServiceUnavailable, gapi_exceptions.TooManyRequests,
                        gapi_exceptions.ResourceExhausted, gapi_exceptions.DeadlineExceeded,
                        gapi_exceptions.InternalServerError)):          # Document AI transient
        return True
    return False


@retry(
    retry=retry_if_exception(is_transient),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def extract_one(pdf_uri: str, pdf_bytes: bytes):
    """OCR the packet, then have Gemini read image + OCR text.
    Returns (packet, page_conf, page_text)."""
    full_text, page_text, page_conf = ocr_document(pdf_bytes)

    resp = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_uri(file_uri=pdf_uri, mime_type="application/pdf"),
            f"OCR TEXT (from Document AI — use for exact numbers/IDs):\n{full_text}",
            PROMPT,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=Packet,
            temperature=0,
        ),
    )
    um = resp.usage_metadata
    if um is not None:
        with _usage_lock:
            USAGE["in_tokens"] += getattr(um, "prompt_token_count", 0) or 0
            USAGE["out_tokens"] += getattr(um, "candidates_token_count", 0) or 0
    packet = resp.parsed if resp.parsed is not None else Packet(**json.loads(resp.text))
    return packet, page_conf, page_text


def already_done() -> set:
    """Load ids that already have an output file — so we can skip them."""
    done = set()
    for b in storage_client.list_blobs(BUCKET, prefix=JSON_PREFIX):
        if b.name.endswith(".json"):
            done.add(b.name[len(JSON_PREFIX):-len(".json")])
    return done


def print_cost_summary(processed: int):
    """Print exact usage for THIS run + an approximate cost (rates are editable constants)."""
    ocr_cost = USAGE["ocr_pages"] / 1_000 * DOCAI_OCR_PER_1K_PAGES
    in_cost = USAGE["in_tokens"] / 1_000_000 * GEMINI_INPUT_PER_1M
    out_cost = USAGE["out_tokens"] / 1_000_000 * GEMINI_OUTPUT_PER_1M
    total = ocr_cost + in_cost + out_cost
    print("---- usage this run (counts exact, $ approximate) ----")
    print(f"  Document AI  : {USAGE['ocr_pages']:,} pages   ~${ocr_cost:.4f}")
    print(f"  Gemini input : {USAGE['in_tokens']:,} tokens  ~${in_cost:.4f}")
    print(f"  Gemini output: {USAGE['out_tokens']:,} tokens  ~${out_cost:.4f}")
    print(f"  TOTAL (est)  : ~${total:.4f}")
    if processed:
        print(f"  Per doc      : ~${total / processed:.5f}   →  19,000 docs ≈ ${total / processed * 19000:.2f}")


def process_blob(blob):
    """Worker (runs in a thread): OCR + extract ONE pdf and write its JSON.
    Returns (status, load_id, detail) where status is 'ok' | 'invalid' | 'failed'.
    No shared mutable state except USAGE, which is guarded by _usage_lock inside
    ocr_document/extract_one. Each PDF's JSON is an independent object, so concurrent
    writes never collide and resumability is preserved."""
    load_id = load_id_from_pdf(blob.name)
    pdf_uri = f"gs://{BUCKET}/{blob.name}"
    try:
        pdf_bytes = blob.download_as_bytes()
    except Exception as e:                           # noqa: BLE001
        return ("failed", load_id, f"download: {e}")

    # Pre-flight: skip broken PDFs before spending an OCR + Gemini call.
    try:
        validate_pdf(pdf_bytes)
    except Exception as e:
        bucket.blob(f"logs/invalid/{load_id}.txt").upload_from_string(str(e))
        return ("invalid", load_id, str(e))

    try:
        packet, page_conf, page_text = extract_one(pdf_uri, pdf_bytes)
        record = packet.model_dump()
        record["load_id"] = load_id
        record["source_pdf"] = pdf_uri
        # Confidence + cross-check, each scoped to the document's OWN pages.
        for doc_obj, doc_dict in zip(packet.documents, record["documents"]):
            doc_dict["ocr_confidence"] = ocr_confidence_for(doc_obj.page_range, page_conf)
            scoped = _norm(page_text_for(doc_obj.page_range, page_text))
            doc_dict["unverified_fields"] = verify_document(doc_dict, scoped)
        bucket.blob(f"{JSON_PREFIX}{load_id}.json").upload_from_string(
            json.dumps(record), content_type="application/json"
        )
        return ("ok", load_id, "")
    except Exception as e:                           # noqa: BLE001 — isolate per-doc failures
        bucket.blob(f"logs/failed/{load_id}.txt").upload_from_string(str(e))
        return ("failed", load_id, str(e))


def main():
    # ONE listing of the PDFs — reused for the multi-PDF check AND the work list.
    all_pdfs = [b for b in storage_client.list_blobs(BUCKET, prefix=PDF_PREFIX)
                if b.name.lower().endswith(".pdf")]
    # list_blobs returns ascending load number (oldest first); sort newest-first if configured.
    all_pdfs.sort(key=lambda b: _load_num(b.name), reverse=NEWEST_FIRST)
    skip_multi = find_multi_pdf_loads(all_pdfs)
    done = already_done()                            # separate prefix (json/); listed once
    print(f"{len(all_pdfs)} PDFs found; {len(done)} loads already done; "
          f"{len(skip_multi)} multi-PDF loads skipped.")

    # Build the work list (skip already-done + multi-PDF), capped at LIMIT.
    work = []
    for b in all_pdfs:
        load_id = load_id_from_pdf(b.name)
        if load_id in done or load_id in skip_multi:
            continue
        work.append(b)
        if LIMIT is not None and len(work) >= LIMIT:
            break

    print(f"Processing {len(work)} loads with {MAX_WORKERS} concurrent workers...")
    ok = failed = invalid = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_blob, b): b for b in work}
        for i, fut in enumerate(as_completed(futures), 1):
            b = futures[fut]
            try:
                status, load_id, detail = fut.result()
            except Exception as e:                   # noqa: BLE001 — a worker crashed unexpectedly
                failed += 1
                print(f"[FAIL] {load_id_from_pdf(b.name)}: worker crashed: {e}", file=sys.stderr)
                continue
            if status == "ok":
                ok += 1
                print(f"[ok] {load_id}")
            elif status == "invalid":
                invalid += 1
                print(f"[skip invalid] {load_id}: {detail}", file=sys.stderr)
            else:
                failed += 1
                print(f"[FAIL] {load_id}: {detail}", file=sys.stderr)
            if i % 50 == 0:
                print(f"  ...{i}/{len(work)}  (ok={ok} failed={failed} invalid={invalid})")

    print(f"Done. ok={ok}, failed={failed}, invalid={invalid} of {len(work)} attempted.")
    print_cost_summary(ok)


if __name__ == "__main__":
    main()
