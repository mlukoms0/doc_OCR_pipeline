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
import sys
from collections import defaultdict
from pathlib import Path
from typing import List, Optional

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
LIMIT            = 100                  # PILOT: 100. Full run: set to None
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


# ---- The schema Gemini must return (mirror of schema/extraction_schema.json) ----
# NOTE: ocr_confidence is NOT here — the pipeline adds it after extraction, from
# Document AI, so Gemini is never asked to invent it.
class Document(BaseModel):
    doc_type: str
    page_range: str
    confidence: float
    illegible: bool = False

    load_number: Optional[str] = None
    pro_number: Optional[str] = None
    bol_number: Optional[str] = None
    order_number: Optional[str] = None

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
- Dates must be YYYY-MM-DD. Money must be plain numbers (no $ or commas).
Return only JSON matching the schema."""


def load_id_from_pdf(blob_name: str) -> str:
    """<PDF_PREFIX>/<load_id>/<file>.pdf  ->  <load_id>"""
    return blob_name[len(PDF_PREFIX):].split("/")[0]


def find_multi_pdf_loads() -> set:
    """Group PDFs by load. Loads with exactly one PDF are processed; loads with more
    than one are RECORDED and skipped for now (multi-doc support comes later).
    Never raises. Returns the set of load_ids to skip."""
    loads = defaultdict(list)
    for blob in storage_client.list_blobs(BUCKET, prefix=PDF_PREFIX):
        if not blob.name.lower().endswith(".pdf"):
            continue
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
    """Run Document AI OCR. Returns (full_text, per_page_mean_confidence)."""
    name = docai_client.processor_path(PROJECT_ID, DOCAI_LOCATION, OCR_PROCESSOR_ID)
    result = docai_client.process_document(
        request=documentai.ProcessRequest(
            name=name,
            raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf"),
        )
    )
    doc = result.document
    page_conf = []
    for page in doc.pages:
        confs = [t.layout.confidence for t in page.tokens if t.layout and t.layout.confidence]
        page_conf.append(round(sum(confs) / len(confs), 3) if confs else 0.0)
    return doc.text, page_conf


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
    """OCR the packet, then have Gemini read image + OCR text. Returns (packet, page_conf)."""
    ocr_text, page_conf = ocr_document(pdf_bytes)

    resp = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_uri(file_uri=pdf_uri, mime_type="application/pdf"),
            f"OCR TEXT (from Document AI — use for exact numbers/IDs):\n{ocr_text}",
            PROMPT,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=Packet,
            temperature=0,
        ),
    )
    packet = resp.parsed if resp.parsed is not None else Packet(**json.loads(resp.text))
    return packet, page_conf


def already_done() -> set:
    """Load ids that already have an output file — so we can skip them."""
    done = set()
    for b in storage_client.list_blobs(BUCKET, prefix=JSON_PREFIX):
        if b.name.endswith(".json"):
            done.add(b.name[len(JSON_PREFIX):-len(".json")])
    return done


def main():
    skip_multi = find_multi_pdf_loads()

    done = already_done()
    print(f"{len(done)} loads already processed — skipping those.")

    processed = 0
    for b in storage_client.list_blobs(BUCKET, prefix=PDF_PREFIX):
        if not b.name.lower().endswith(".pdf"):
            continue
        load_id = load_id_from_pdf(b.name)
        if load_id in done:
            continue
        if load_id in skip_multi:
            continue          # multiple PDFs in this folder — deferred to multi-doc support
        if LIMIT is not None and processed >= LIMIT:
            print(f"Reached LIMIT ({LIMIT}). Stopping.")
            break

        pdf_uri = f"gs://{BUCKET}/{b.name}"
        pdf_bytes = b.download_as_bytes()

        # Pre-flight: skip broken PDFs before spending an OCR + Gemini call.
        try:
            validate_pdf(pdf_bytes)
        except Exception as e:
            bucket.blob(f"logs/invalid/{load_id}.txt").upload_from_string(str(e))
            print(f"[skip invalid] {load_id}: {e}", file=sys.stderr)
            continue

        try:
            packet, page_conf = extract_one(pdf_uri, pdf_bytes)
            record = packet.model_dump()
            record["load_id"] = load_id
            record["source_pdf"] = pdf_uri
            # Attach Document AI's confidence to each document (trustworthy signal).
            for doc_obj, doc_dict in zip(packet.documents, record["documents"]):
                doc_dict["ocr_confidence"] = ocr_confidence_for(doc_obj.page_range, page_conf)

            bucket.blob(f"{JSON_PREFIX}{load_id}.json").upload_from_string(
                json.dumps(record), content_type="application/json"
            )
            done.add(load_id)
            processed += 1
            if processed % 25 == 0:
                print(f"  ...{processed} processed")
            print(f"[ok] {load_id}")
        except Exception as e:                       # noqa: BLE001 — isolate per-doc failures
            bucket.blob(f"logs/failed/{load_id}.txt").upload_from_string(str(e))
            print(f"[FAIL] {load_id}: {e}", file=sys.stderr)

    print(f"Done. {processed} new loads processed this run.")


if __name__ == "__main__":
    main()
