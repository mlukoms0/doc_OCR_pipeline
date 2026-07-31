"""
Freight packet extraction — Document AI OCR + Gemini, one pass per PDF.

Per PDF:
  - Validate the PDF opens and has pages (skip broken files)
  - Document AI OCR -> reliable text layer + per-page confidence
  - Render the pages locally at RENDER_DPI as JPEGs (cheaper than shipping the
    raw PDF: Gemini bills images by tile count, so DPI drives input cost)
  - Gemini reads the IMAGES + the PAGE-MARKED OCR TEXT together -> structured JSON
    (images for layout/classification, OCR text for exact numbers/IDs)
  - Attach Document AI's confidence to each extracted document
  - Write gs://<BUCKET>/<JSON_PREFIX>/<load_id>.json  (resumable)

Request layout is deliberate: the static PROMPT goes FIRST so the prefix is
identical across every call, then the per-PDF images, then the OCR text.

Each document is extracted from ITS OWN pages only — values are never carried
from one document to another here. Cross-document fill (e.g. stamping the rate
con's load_number onto the BOL) happens in the gold layer, where it is auditable.

Run the PILOT first (LIMIT = 100), review in BigQuery, then set LIMIT = None.
"""

import csv
import json
import re
import sys
import threading
import time
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
from pydantic import BaseModel, Field
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

# ============ EDIT THESE VALUES ============
PROJECT_ID       = "144240581301"
LOCATION         = "us-east1"         
BUCKET           = "doc-archive67"
#Set limit = <int> for test runs, = None for full run.
LIMIT            = None     
#Doc concurrency scans. Limit to avoid api request errors. default = 12
MAX_WORKERS      = 10
NEWEST_FIRST     = False                 
DOCAI_LOCATION   = "us"
OCR_PROCESSOR_ID = "a889dd87be6492b0"

# Page images sent to Gemini.
MAX_PAGE_PX      = 1536   # long-edge cap in pixels; keep <= 1536 to stay at 4 tiles
JPEG_QUALITY     = 75

# Cache the Document AI OCR result in GCS, keyed by load_id, and reuse it on re-runs.
CACHE_OCR        = True

# Max pages per synchronous Document AI call. The API allows 15 in normal mode and 30 in
# IMAGELESS mode; ocr_document() sets imageless_mode=True  30 without switching to batch_process_documents.
OCR_PAGE_LIMIT   = 30

# -1 = dynamic thinking 
# 0 = thinking off, cheapest.
# 
THINKING_BUDGET   = -1
MAX_OUTPUT_TOKENS = 16384
# ===========================================

#PDF_PREFIX - point to the GCS Bucket folder where raw docs are stored. 

PDF_PREFIX  = "raw/loads/"
# Output prefix. BUMP THIS instead of deleting, whenever the prompt or schema changes.
#
# Point `bq load` at whichever prefix you want in BigQuery.

JSON_PREFIX = "json_final/"
OCR_PREFIX  = "ocr/"          # cached Document AI output, one <load_id>.json per packet
MODEL       = "gemini-2.5-flash"

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET)
docai_client = documentai.DocumentProcessorServiceClient(
    client_options={"api_endpoint": f"{DOCAI_LOCATION}-documentai.googleapis.com"}
)

# ---- Usage tally (exact counts) + rough cost estimate ----
# Rates are approximate
# current Vertex AI + Document AI pricing pages (07/26)

GEMINI_INPUT_PER_1M    = 0.30    # Gemini 2.5 Flash, per 1M input tokens
GEMINI_OUTPUT_PER_1M   = 2.50    # per 1M output tokens (thinking bills at THIS rate)
DOCAI_OCR_PER_1K_PAGES = 1.50    # Document AI OCR, per 1,000 pages
BACKFILL_LOADS         = 17112
USAGE = {
    "ocr_pages": 0, "in_tokens": 0, "out_tokens": 0, "think_tokens": 0,
    "total_tokens": 0,       # the SDK's own total, used to self-check the components
    "ocr_cache_hits": 0,     # packets served from the OCR cache 
    "no_usage_meta": 0,      # calls that reported no usage at all -> tokens we cannot see
    "tally_mismatch": 0,     # components != the SDK total -> a missed category
    "truncated": 0,          # calls that stopped for any reason other than STOP
}
_usage_lock = threading.Lock()


class Stop(BaseModel):
    sequence: int
    stop_type: Literal["pickup", "delivery"]
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    scheduled_date: Optional[str] = None                     # YYYY-MM-DD
    appt_start: Optional[str] = None                         # HH:MM, 24-hour
    appt_end: Optional[str] = None                           # HH:MM, 24-hour
    appt_type: Optional[Literal["appointment", "fcfs"]] = None
    reference_number: Optional[str] = None                   # PU#/DEL#/appt# for THIS stop


class Accessorial(BaseModel):
    
    type: Literal[
        "detention_pickup", "detention_delivery", "layover", "tonu", "lumper",
        "stop_off", "driver_assist", "tarp", "reconsignment", "unloading", "other",
    ]
    amount: Optional[float] = None
    notes: Optional[str] = None


class Reference(BaseModel):
    """Any other identifying number on the document. """
    ref_type: Literal["po", "pickup", "delivery", "appointment", "customer",
                      "trailer", "container", "seal", "other"]
    value: str


class Document(BaseModel):
    # Literal (not str) so the response schema actually CONSTRAINS Gemini to these values.
    doc_type: Literal[
        "rate_confirmation", "bill_of_lading", "proof_of_delivery",
        "invoice", "lumper_receipt", "packing_list", "other",
    ]
    page_range: str
    illegible: bool = False
    is_signed_delivery_copy: Optional[bool] = None
    delivery_signed_date: Optional[str] = None               # YYYY-MM-DD

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

    # First pickup / last delivery
    # every intermediate stop is in `stops`.
    pickup_date: Optional[str] = None      # YYYY-MM-DD
    delivery_date: Optional[str] = None
    # Appointment windows. 
    pickup_appt_start: Optional[str] = None      # HH:MM 24-hour
    pickup_appt_end: Optional[str] = None
    delivery_appt_start: Optional[str] = None
    delivery_appt_end: Optional[str] = None

    rate_total: Optional[float] = None
    line_haul: Optional[float] = None
    fuel_surcharge: Optional[float] = None
    # docs classified overwrites is_all_in. 
    rate_is_all_in: Optional[bool] = None
    currency: Optional[str] = None
    # Loaded miles as printed. 
    miles: Optional[float] = None
    payment_net_days: Optional[int] = None
    quick_pay_pct: Optional[float] = None

    commodity: Optional[str] = None
    weight: Optional[float] = None
    weight_unit: Optional[Literal["lb", "kg"]] = None
    pieces: Optional[int] = None
    pallet_count: Optional[int] = None
    pallet_spaces: Optional[int] = None
    equipment_class: Optional[Literal[
        "dry_van", "reefer", "flatbed", "step_deck", "power_only",
        "container", "tanker", "other",
    ]] = None
    equipment_length_ft: Optional[int] = None
    is_team: Optional[bool] = None
    equipment_type_raw: Optional[str] = None     # verbatim string, so nothing is lost
    temperature_setpoint_f: Optional[float] = None
    temperature_mode: Optional[Literal["continuous", "cycle"]] = None
    freight_class: Optional[str] = None
    hazmat: Optional[bool] = None
    seal_number: Optional[str] = None

    delivered: Optional[bool] = None
    received_by: Optional[str] = None
    signature_present: Optional[bool] = None
    exceptions_notes: Optional[str] = None
    # What the receiver actually signed for, vs what shipped. The gap IS the claim.
    pieces_received: Optional[int] = None
    osd_code: Optional[Literal["clear", "shortage", "overage", "damage", "refused"]] = None

    stops: List[Stop] = []
    accessorials: List[Accessorial] = []
    references: List[Reference] = []

    rate_total_evidence: Optional[str] = None    # e.g. "$1,600.00"
    weight_evidence: Optional[str] = None        # e.g. "32,268 LBS"


class Packet(BaseModel):
    packet_summary: str
    documents: List[Document]


PROMPT = """You are extracting data from a SCANNED freight load packet: a single PDF
of 2-5 pages that may contain a rate confirmation, a bill of lading (BOL), a proof of
delivery (POD), an invoice, and/or a lumper receipt.

You are given BOTH the page images AND OCR text extracted from the same PDF by a
dedicated OCR engine. The OCR text is split by "--- PAGE n ---" markers that correspond
to the page images in order. Use the OCR text to read exact numbers, IDs, and codes
precisely; use the images for layout, document boundaries, classification, and page
ranges. If they conflict, prefer whichever is clearly correct.

Rules:
- Identify EACH distinct document in the packet: its type, and the page range it covers.
- EXTRACT ONLY WHAT IS PRINTED ON THAT DOCUMENT'S OWN PAGES. Never carry a value from one
  document to another. If the rate confirmation shows a load number but the BOL does not
  print it, the BOL's load_number is absent — do NOT copy it across. Same for carrier,
  broker, weight and dates. Documents are reconciled downstream; report each one as it
  stands on its own pages.
- Use null for anything not present. If a value is present but you cannot read it clearly,
  use null and do NOT guess — especially numbers (rates, weights, MC/DOT numbers). If
  anything on a document was hard to read, set "illegible": true.
  (An "omit the key entirely" rule used to live here. It was removed after being measured
  as a no-op: with a response_schema, constrained decoding emits every property in the
  schema whether or not it is `required`, so 41% of a real document still came back as
  explicit nulls. The rule bought nothing and cost prompt tokens plus one more thing for
  the model to reason about. The null padding is ~168 tokens per document, about $16
  across the backfill — real, but not reachable from the prompt.)
- doc_type MUST be one of the allowed enum values. A rate confirmation is sometimes titled
  "Load Confirmation", "Carrier Confirmation", or "Load Sheet" — all of those are
  rate_confirmation. Anything that fits none of the values is "other".
- THE SIGNED BOL IS THE PROOF OF DELIVERY. In a freight packet the POD is usually the
  bill of lading itself, signed and stamped at the receiver. Do NOT change doc_type for
  that — a BOL is still a bill_of_lading. Instead set is_signed_delivery_copy = true
  whenever the document carries a receiver's signature, a delivery stamp, a "received
  in good condition" mark, or a printed receiver name in the consignee/received-by
  block, and put the date beside that signature in delivery_signed_date. A packet often
  contains the SAME BOL twice — a clean pickup copy and a signed delivery copy; report
  both, and set the flag only on the signed one.
- carrier_name and broker_name are COMPANY names, never a person. "Austin Young" is a
  rep; the brokerage is whatever company name appears on the letterhead. Likewise
  "Dmitriy at DB7" is the company DB7, not the person.
- Dates must be YYYY-MM-DD. Money and weights must be plain numbers (no $ or commas);
  weight is a whole number of pounds unless the document clearly prints a fraction.
- Pallets, spaces and pieces are THREE DIFFERENT things — never put one number in two:
  * pallet_count  = physical PALLETS / skids (Pallets, PLT, PLTS, Skids). If the COMMODITY
    text states a pallet quantity, that IS pallet_count: "30 PALLETS DRY PRODUCT" ->
    pallet_count 30, pieces omitted. "15 PALLETS OF CABINET PARTS" -> pallet_count 15.
  * pallet_spaces = trailer POSITIONS / SPACES (Spaces, SPC, Positions, Spots, linear feet).
    Only when the document prints a distinct spaces figure; never a copy of pallet_count.
  * pieces        = CARTONS / cases / units (Pieces, Units, Cases, Cartons, Qty). A "pallet"
    count in the hundreds is really a carton count — put it in pieces.
- Money:
  * rate_total = the TOTAL the carrier is paid, all-in.
  * line_haul = ONLY when the document prints a separate line-haul item that is a different
    number from the total. NEVER copy rate_total into line_haul.
  * fuel_surcharge = ONLY a fuel/FSC line item that is part of that same total. A fuel
    surcharge SCHEDULE or rate table is not a charge — ignore it.
  * rate_is_all_in — ignore this field, a downstream rule sets it. Just leave line_haul
    and fuel_surcharge empty when the page prints only a single total.
  * accessorials = every OTHER charge line: detention, layover, TONU, lumper, stop-off,
    driver assist, tarp, reconsignment. One entry each, with its amount.
  * miles = loaded miles as printed on the document. Do not calculate it.
- stops = the FULL itinerary, in order, one entry per pickup and per delivery, including
  intermediate stops. sequence starts at 1. Also fill origin_*/destination_* from the
  first pickup and last delivery. Times are 24-hour HH:MM; appt_type is "appointment"
  when a specific time is booked and "fcfs" for first-come-first-served or a window.
- equipment: put the verbatim string in equipment_type_raw, then split it —
  "53' Dry Van" -> equipment_class dry_van, equipment_length_ft 53.
  "V" or "Van" with no length -> dry_van, equipment_length_ft omitted.
  REEFER LOADS ALWAYS STATE A TEMPERATURE somewhere on the rate confirmation, and it is
  rarely in the equipment field — look for "Temp", "Set at", "Reefer Temp", "Continuous",
  "Cycle", or a bare value like "-10F" / "34 degrees" anywhere on the page, including the
  notes and special-instructions block. "Reefer -10F continuous" -> equipment_class reefer,
  temperature_setpoint_f -10, temperature_mode continuous. Fahrenheit unless the document
  says Celsius; convert Celsius to Fahrenheit.
- references = other numbers on the document that identify something: PO, pickup, delivery,
  appointment, customer, trailer, container, seal. Do not put these in order_number — that
  field is for the document's own order number only.
  BE SELECTIVE. If you cannot tell what a number MEANS, leave it out — do not record it
  with ref_type "other". A scanned freight page is covered in numbers (fax headers, form
  codes, zip codes, phone numbers, page counts) and none of those are references. Record
  the same value ONCE per document, never twice. A reference must be an actual identifier:
  never record "0", a single digit, or an empty box as a reference value.
- EVIDENCE fields: for rate_total and weight, also return the exact text you read the
  number from, verbatim, including punctuation and units — rate_total_evidence
  "$1,600.00", weight_evidence "32,268 LBS". Copy the characters as printed; do not
  reformat them. These are checked against the OCR text automatically.

Worked examples:
  Rate con line "LINE HAUL 5,415.20 / FUEL 1,084.80 / TOTAL 6,500.00"
    -> rate_total 6500, line_haul 5415.20, fuel_surcharge 1084.80,
       rate_total_evidence "6,500.00"
  Rate con line "TOTAL CARRIER PAY: $3,500.00" and nothing else
    -> rate_total 3500, line_haul and fuel_surcharge OMITTED
  Commodity "30 PALLETS DRY PRODUCT", weight "32,955 LBS"
    -> pallet_count 30, commodity "30 PALLETS DRY PRODUCT", weight 32955, weight_unit lb,
       weight_evidence "32,955 LBS", pieces OMITTED
  A BOL page with "RECEIVED BY: M. Alvarez  01/08/2020" hand-written at the bottom
    -> doc_type bill_of_lading, is_signed_delivery_copy true,
       delivery_signed_date "2020-01-08", received_by "M. Alvarez", signature_present true
- The RATE CONFIRMATION carries the most reliable version of a load's details, so read it
  thoroughly — but still only its own pages. From rate confirmations capture: load_number
  as printed here; rc_number (the number after "Load #", "Confirmation #", "Rate Con #",
  "Order #" or "Pro #" near the top — OFTEN the SAME number as load_number, in which case
  put it in both; set them differently only when the document really prints two numbers);
  rate_total; total weight; pallet_count/pallet_spaces; the FULL commodity description;
  broker_name.
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
        print(f"[!] {len(multi)} of {len(loads)} Folders have multiple PDFs -Skipped. See {BUCKET}/logs/multi_pdf_loads.txt")
    else:
        print(f"[ok] All {len(loads)} loads have exactly one PDF.")

    return set(multi)


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


_transient_retry = retry(
    retry=retry_if_exception(is_transient),
    wait=wait_exponential(multiplier=2, min=4, max=120),
    stop=stop_after_attempt(8),
    reraise=True,
)


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


def render_pages(pdf_bytes: bytes) -> List[bytes]:
    """Rasterize every page to a JPEG whose LONG EDGE is at most MAX_PAGE_PX. """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        out = []
        for i in range(doc.page_count):
            page = doc[i]
            w, h = page.rect.width, page.rect.height
            longest = max(w, h) or 1
            zoom = min(MAX_PAGE_PX / longest, 4.0)   # never upscale past 4x a tiny page
            # Floor the scaled SHORT edge at 1px; without this a page wider than ~3000:1
            # renders 0px tall and the whole request is rejected.
            if min(w, h) * zoom < 1:
                zoom = max(zoom, 1.0 / max(min(w, h), 0.01))
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom),
                                  colorspace=fitz.csRGB, alpha=False)
            if pix.width < 1 or pix.height < 1:
                raise ValueError(f"page {i + 1} rendered to {pix.width}x{pix.height} "
                                 f"(source {w:.1f}x{h:.1f}pt) - cannot send a 0-px image")
            jpg = pix.tobytes("jpeg", jpg_quality=JPEG_QUALITY)
            if not jpg or jpg[:2] != b"\xff\xd8":
                raise ValueError(f"page {i + 1} did not encode to a valid JPEG")
            out.append(jpg)
        return out
    finally:
        doc.close()


_VERTEX_SCHEMA_KEYS = {
    "type", "format", "description", "nullable", "enum", "items", "properties",
    "required", "propertyOrdering", "anyOf", "$ref", "$defs", "default",
}


def _lean_response_schema() -> dict:
    """Return a copy of Packet.model_json_schema() with only the keys that constrain Gemini's output."""
    def schema_node(node):
        if not isinstance(node, dict):
            return [schema_node(v) for v in node] if isinstance(node, list) else node
        out = {}
        for k, v in node.items():
            if k in ("title",) or k not in _VERTEX_SCHEMA_KEYS:
                continue
            if k in ("properties", "$defs"):
                out[k] = {name: schema_node(sub) for name, sub in v.items()}
            elif k in ("items",):
                out[k] = schema_node(v)
            elif k in ("anyOf",):
                out[k] = [schema_node(sub) for sub in v]
            else:
                out[k] = v
        return out
    return schema_node(Packet.model_json_schema())


RESPONSE_SCHEMA = _lean_response_schema()


def ocr_cache_path(load_id: str) -> str:
    return f"{OCR_PREFIX}{load_id}.json"


def load_cached_ocr(load_id: str):
    """Return (full_text, page_text, page_conf) from GCS, or None if not cached."""
    
    if not CACHE_OCR:
        return None
    blob = bucket.blob(ocr_cache_path(load_id))
    try:
        if not blob.exists():
            return None
        d = json.loads(blob.download_as_bytes())
        return d["full_text"], d["page_text"], d["page_conf"]
    except Exception:                                # noqa: BLE001 — cache miss is never fatal
        return None


def save_cached_ocr(load_id: str, full_text: str, page_text, page_conf) -> None:
    if not CACHE_OCR:
        return
    try:
        bucket.blob(ocr_cache_path(load_id)).upload_from_string(
            json.dumps({"full_text": full_text, "page_text": page_text, "page_conf": page_conf}),
            content_type="application/json",
        )
    except Exception:                                # noqa: BLE001 — caching is best-effort
        pass


@_transient_retry
def ocr_document(pdf_bytes: bytes):
    """Run Document AI OCR. Returns (full_text, per_page_text, per_page_mean_confidence).
    per_page_text lets the cross-check verify a document against ONLY its own pages."""
    name = docai_client.processor_path(PROJECT_ID, DOCAI_LOCATION, OCR_PROCESSOR_ID)
    def _one_call(chunk_bytes: bytes):
        """OCR up to OCR_PAGE_LIMIT pages. Returns (text, [page_text], [page_conf])."""
        result = docai_client.process_document(
            request=documentai.ProcessRequest(
                name=name,
                raw_document=documentai.RawDocument(
                    content=chunk_bytes, mime_type="application/pdf"),
                imageless_mode=True,
            )
        )
        doc = result.document
        text = doc.text or ""
        with _usage_lock:
            USAGE["ocr_pages"] += len(doc.pages)   # every page here is a billable OCR page
        confs_out, text_out = [], []
        for page in doc.pages:
            confs = [t.layout.confidence for t in page.tokens if t.layout and t.layout.confidence]
            confs_out.append(round(sum(confs) / len(confs), 3) if confs else 0.0)
            # slice this page's own text out of THIS CHUNK's text (offsets are chunk-relative)
            segs = page.layout.text_anchor.text_segments if page.layout and page.layout.text_anchor else []
            text_out.append("".join(text[int(s.start_index):int(s.end_index)] for s in segs))
        return text, text_out, confs_out

    
    doc_pages = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        n_pages = doc_pages.page_count
    finally:
        doc_pages.close()

    if n_pages <= OCR_PAGE_LIMIT:
        return _one_call(pdf_bytes)

    # Oversized packet: OCR it in page-ordered chunks and stitch the results.
    
   
    full_text, page_text, page_conf = "", [], []
    src = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        for start in range(0, n_pages, OCR_PAGE_LIMIT):
            end = min(start + OCR_PAGE_LIMIT, n_pages) - 1
            part = fitz.open()
            try:
                part.insert_pdf(src, from_page=start, to_page=end)
                chunk_bytes = part.tobytes()
            finally:
                part.close()
            t, pt, pc = _one_call(chunk_bytes)
            full_text += ("\n" if full_text else "") + t
            page_text.extend(pt)
            page_conf.extend(pc)
    finally:
        src.close()
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
# (money + identifiers). 
KEY_FIELDS = [
    "load_number", "pro_number", "bol_number", "order_number", "rc_number",
    "broker_mc", "carrier_mc", "carrier_dot", "carrier_scac",
    "rate_total", "line_haul", "fuel_surcharge", "weight",
]

# Fields the model returns VERBATIM source text for.
EVIDENCE_FIELDS = {"rate_total": "rate_total_evidence", "weight": "weight_evidence"}

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
        # Prefer the model's verbatim evidence string when it gave one 
        ev_key = EVIDENCE_FIELDS.get(field)
        evidence = doc_dict.get(ev_key) if ev_key else None
        # LENGTH GUARD. 
        if evidence and len(str(evidence)) <= 64:
            if _norm(evidence) not in norm_text:
                unverified.append(field)
            continue
        norm_value = _norm(value)
        if len(norm_value) < 3:
            continue                       # too short to verify without false alarms
        if norm_value not in norm_text:
            unverified.append(field)
    return ",".join(unverified)


def marked_ocr_text(full_text: str, page_text) -> str:
    """OCR text with '--- PAGE n ---' markers so Gemini can tell where each page starts."""
    if not any(t.strip() for t in page_text):
        return full_text                        
    return "\n".join(f"--- PAGE {i} ---\n{t}" for i, t in enumerate(page_text, 1))


@_transient_retry
def gemini_extract(page_images: List[bytes], full_text: str, page_text) -> Packet:
    """One Gemini call: page images + page-marked OCR text -> structured Packet.

    Content order is deliberate. The static PROMPT goes FIRST so every one of the 19k
    calls shares an identical prefix; the per-PDF images and OCR text follow. Previously
    the PDF came first, which put variable bytes at the head of every request."""
    contents = [PROMPT]
    contents += [types.Part.from_bytes(data=img, mime_type="image/jpeg") for img in page_images]
    contents.append(
        "OCR TEXT (from Document AI — use for exact numbers/IDs; the PAGE markers match "
        f"the images above, in order):\n{marked_ocr_text(full_text, page_text)}"
    )


    def _payload_report() -> str:
        dims = []
        for i, img in enumerate(page_images, 1):
            w = h = 0
            try:                                  # read dimensions straight out of the JPEG
                j = 2
                while j < len(img) - 9:
                    if img[j] != 0xFF:
                        j += 1; continue
                    if img[j + 1] in (0xC0, 0xC1, 0xC2):
                        h = int.from_bytes(img[j + 5:j + 7], "big")
                        w = int.from_bytes(img[j + 7:j + 9], "big")
                        break
                    j += 2 + int.from_bytes(img[j + 2:j + 4], "big")
            except Exception:                     # noqa: BLE001 
                pass
            dims.append(f"p{i}:{w}x{h}/{len(img) // 1024}KB")
        text = marked_ocr_text(full_text, page_text)
        return (f"pages={len(page_images)} images=[{' '.join(dims)}] "
                f"image_bytes={sum(len(i) for i in page_images):,} "
                f"ocr_chars={len(text):,} prompt_chars={len(PROMPT):,} "
                f"schema_chars={len(json.dumps(RESPONSE_SCHEMA)):,} "
                f"total_inline={(sum(len(i) for i in page_images) + len(text)):,}B")

    try:
        resp = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RESPONSE_SCHEMA,   # see _lean_response_schema
                temperature=0,
                max_output_tokens=MAX_OUTPUT_TOKENS,
                thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET),
            ),
        )
    except genai_errors.ClientError as e:
        if getattr(e, "code", None) == 400:
            raise ValueError(f"400 INVALID_ARGUMENT from Vertex. Payload was: "
                             f"{_payload_report()}. Original: {e}") from e
        raise

    um = resp.usage_metadata
    if um is None:
        # Never silently skip
        with _usage_lock:
            USAGE["no_usage_meta"] += 1
    else:
        prompt_t = getattr(um, "prompt_token_count", 0) or 0
        cand_t = getattr(um, "candidates_token_count", 0) or 0
        think_t = getattr(um, "thoughts_token_count", 0) or 0
        total_t = getattr(um, "total_token_count", 0) or 0
        tool_t = getattr(um, "tool_use_prompt_token_count", 0) or 0
        with _usage_lock:
            USAGE["in_tokens"] += prompt_t
            USAGE["out_tokens"] += cand_t
            USAGE["think_tokens"] += think_t
            USAGE["total_tokens"] += total_t
            # SELF-CHECK. total = prompt + candidates + tool_use + thoughts. 
            if total_t and abs(total_t - (prompt_t + cand_t + think_t + tool_t)) > 1:
                USAGE["tally_mismatch"] += 1

    # TRUNCATION CHECK. 
    fr = None
    if resp.candidates:
        fr = getattr(resp.candidates[0], "finish_reason", None)
    fr_name = getattr(fr, "name", None) or (str(fr) if fr is not None else None)
    if fr_name and fr_name.upper() not in ("STOP", "FINISH_REASON_STOP"):
        with _usage_lock:
            USAGE["truncated"] += 1
        raise ValueError(
            f"Gemini stopped with finish_reason={fr_name} (not STOP): the response is "
            f"incomplete and MUST NOT be trusted. prompt={getattr(um,'prompt_token_count',0)} "
            f"thinking={getattr(um,'thoughts_token_count',0)} output={getattr(um,'candidates_token_count',0)} "
            f"of max_output_tokens={MAX_OUTPUT_TOKENS}. Raise MAX_OUTPUT_TOKENS or lower "
            f"THINKING_BUDGET (thinking is charged against the same ceiling)."
        )
    
    call_stats = {
        "in": getattr(um, "prompt_token_count", 0) or 0 if um else 0,
        "out": getattr(um, "candidates_token_count", 0) or 0 if um else 0,
        "think": getattr(um, "thoughts_token_count", 0) or 0 if um else 0,
    }
    parsed = resp.parsed
    if isinstance(parsed, Packet):
        return parsed, call_stats
    if isinstance(parsed, dict):
        return Packet(**parsed), call_stats
    return Packet(**json.loads(resp.text)), call_stats


def extract_one(load_id: str, pdf_bytes: bytes):
    """OCR the packet (cached), render its pages, then have Gemini read images + text.
    Each remote step retries on its own, so a Gemini rate limit never re-pays for OCR.
    Returns (packet, page_conf, page_text, ocr_was_cached)."""
    cached = load_cached_ocr(load_id)
    if cached is not None:
        full_text, page_text, page_conf = cached
    else:
        full_text, page_text, page_conf = ocr_document(pdf_bytes)
        save_cached_ocr(load_id, full_text, page_text, page_conf)
    page_images = render_pages(pdf_bytes)
    packet, call_stats = gemini_extract(page_images, full_text, page_text)
    call_stats["ocr_cached"] = cached is not None
    call_stats["pages"] = len(page_images)
    return packet, page_conf, page_text, call_stats


def summarize_docs(packet) -> str:
    """'rate_confirmation:2p bill_of_lading x2:2p POD' — what the packet split into."""

    
    agg = defaultdict(lambda: [0, 0])          # doc_type -> [doc_count, page_count]
    has_pod = False
    for d in packet.documents:
        entry = agg[d.doc_type]
        entry[0] += 1
        entry[1] += len(pages_from_range(d.page_range))
        if d.is_signed_delivery_copy or d.doc_type == "proof_of_delivery":
            has_pod = True
    parts = [f"{dt}{'' if n == 1 else f' x{n}'}:{p}p"
             for dt, (n, p) in sorted(agg.items(), key=lambda kv: (-kv[1][1], kv[0]))]
    if has_pod:
        parts.append("POD")
    return " ".join(parts)


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
    think_cost = USAGE["think_tokens"] / 1_000_000 * GEMINI_OUTPUT_PER_1M   # output rate
    total = ocr_cost + in_cost + out_cost + think_cost
    print("---- usage this run (counts exact, $ approximate) ----")
    print(f"  Document AI  : {USAGE['ocr_pages']:,} pages   ~${ocr_cost:.4f}"
          f"   ({USAGE['ocr_cache_hits']:,} packets served from the OCR cache, $0)")
    print(f"  Gemini input : {USAGE['in_tokens']:,} tokens  ~${in_cost:.4f}")
    print(f"  Gemini output: {USAGE['out_tokens']:,} tokens  ~${out_cost:.4f}")
    print(f"  Gemini think : {USAGE['think_tokens']:,} tokens  ~${think_cost:.4f}"
          f"   (THINKING_BUDGET={THINKING_BUDGET}; set 0 to switch off)")
    print(f"  TOTAL (est)  : ~${total:.4f}")

    warned = False
    if USAGE["tally_mismatch"]:
        warned = True
        print(f"  [!] {USAGE['tally_mismatch']:,} calls where our component sum != the SDK's "
              f"total_token_count. A billable category is MISSING from this tally - "
              f"compare against the cross-check below and add it.")
    if USAGE["no_usage_meta"]:
        warned = True
        print(f"  [!] {USAGE['no_usage_meta']:,} calls reported no usage metadata. Those "
              f"tokens were billed but are NOT in the figures above.")
    if USAGE["truncated"]:
        warned = True
        print(f"  [!] {USAGE['truncated']:,} calls stopped before finishing (finish_reason "
              f"!= STOP) and were rejected. See logs/failed/. Raise MAX_OUTPUT_TOKENS or "
              f"lower THINKING_BUDGET.")
    # Independent cross-check: 
    summed = USAGE["in_tokens"] + USAGE["out_tokens"] + USAGE["think_tokens"]
    if not USAGE["total_tokens"]:
        print("  cross-check  : SKIPPED - the SDK reported no total_token_count.")
    else:
        drift = USAGE["total_tokens"] - summed
        if abs(drift) > max(100, 0.01 * USAGE["total_tokens"]):
            print(f"  [!] cross-check: SDK total {USAGE['total_tokens']:,} vs priced "
                  f"{summed:,} (drift {drift:+,}) <-- UNPRICED TOKENS")
        else:
            print(f"  cross-check  : clean - SDK total {USAGE['total_tokens']:,} vs priced "
                  f"{summed:,} (drift {drift:+,})")

   
    if USAGE["ocr_pages"]:
        print(f"  calibrate    : Document AI billed {USAGE['ocr_pages']:,} pages this run. "
              f"Divide your actual Document AI line item by {USAGE['ocr_pages'] / 1000:.3f} "
              f"to get the true per-1k rate, then set DOCAI_OCR_PER_1K_PAGES.")
    if processed:
        print(f"  Per packet   : ~${total / processed:.5f}   ->  {BACKFILL_LOADS:,} packets "
              f"=~ ${total / processed * BACKFILL_LOADS:.2f}")
        print(f"  NOTE: divisor is the {processed:,} packets that SUCCEEDED. Tokens spent on "
              f"failures are in the totals above but not in this per-packet figure.")


def process_blob(blob):
    """Worker (runs in a thread): OCR + extract ONE pdf and write its JSON.
    Returns (status, load_id, detail) where status is 'ok' | 'invalid' | 'failed'."""
  
    load_id = load_id_from_pdf(blob.name)
    pdf_uri = f"gs://{BUCKET}/{blob.name}"
    try:
        pdf_bytes = blob.download_as_bytes()
    except Exception as e:                           # noqa: BLE001
        return ("failed", load_id, f"download: {e}")

    # Pre-flight: skip broken PDFs 
    try:
        validate_pdf(pdf_bytes)
    except Exception as e:
        bucket.blob(f"logs/invalid/{load_id}.txt").upload_from_string(str(e))
        return ("invalid", load_id, str(e))

    try:
        packet, page_conf, page_text, call_stats = extract_one(load_id, pdf_bytes)
        if call_stats.get("ocr_cached"):
            with _usage_lock:
                USAGE["ocr_cache_hits"] += 1
        record = packet.model_dump()
        record["load_id"] = load_id
        record["source_pdf"] = pdf_uri
        # Confidence + cross-check, each scoped to the document's pages.
        for doc_obj, doc_dict in zip(packet.documents, record["documents"]):
            doc_dict["ocr_confidence"] = ocr_confidence_for(doc_obj.page_range, page_conf)
            scoped = _norm(page_text_for(doc_obj.page_range, page_text))
            doc_dict["unverified_fields"] = verify_document(doc_dict, scoped)
        bucket.blob(f"{JSON_PREFIX}{load_id}.json").upload_from_string(
            json.dumps(record), content_type="application/json"
        )


        docs = record["documents"]
        worst_ocr = min((d["ocr_confidence"] for d in docs
                         if d["ocr_confidence"] is not None), default=None)
        flagged = sorted({f for d in docs for f in (d["unverified_fields"] or "").split(",") if f})
        bits = [summarize_docs(packet)]
        bits.append(f"{call_stats['pages']}p")
        # The ITINERARY
        rc_stops = [len(d.get("stops") or []) for d in docs
                    if d.get("doc_type") == "rate_confirmation"]
        n_stops = max(rc_stops) if any(rc_stops) else max(
            (len(d.get("stops") or []) for d in docs), default=0)
        n_acc = sum(len(d.get("accessorials") or []) for d in docs)
        if n_stops:
            bits.append(f"{n_stops}stop")
        if n_acc:
            bits.append(f"{n_acc}acc")
        if worst_ocr is not None:
            bits.append(f"ocr{worst_ocr:.2f}")
        bits.append(f"out{call_stats['out']:,}")
        bits.append(f"think{call_stats['think']:,}")
        if call_stats.get("ocr_cached"):
            bits.append("cached")
        if flagged:
            bits.append("!" + ",".join(flagged))
        return ("ok", load_id, " | ".join(bits))
    except Exception as e:                           # noqa: BLE001 — isolate per-doc failures
        bucket.blob(f"logs/failed/{load_id}.txt").upload_from_string(str(e))
        return ("failed", load_id, str(e))


class Progress:
    """A status line pinned to the bottom of the terminal while results scroll above it."""

    def __init__(self, total: int):
        self.total = total
        self.ok = self.failed = self.invalid = 0
        self._start = time.monotonic()
        self._lock = threading.Lock()
        self._live = sys.stdout.isatty()
        self._stop = threading.Event()
        self._width = 0
        self._thread = None

    @staticmethod
    def _hms(secs: float) -> str:
        secs = int(max(secs, 0))
        h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
        return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

    def _status(self) -> str:
        done = self.ok + self.failed + self.invalid
        el = time.monotonic() - self._start
        rate = done / el if el > 0 else 0.0                  # packets per second
        eta = (self.total - done) / rate if rate > 0 else 0
        pct = 100.0 * done / self.total if self.total else 0.0
        return (f"[{self._hms(el)}] {done}/{self.total} ({pct:.0f}%)  "
                f"ok={self.ok} failed={self.failed} invalid={self.invalid}  "
                f"{rate * 60:.1f}/min  eta {self._hms(eta)}")

    def _draw(self) -> None:
        """Caller must hold the lock. Pads to the previous width so no stale text shows."""
        if not self._live:
            return
        s = self._status()
        sys.stdout.write("\r" + s + " " * max(0, self._width - len(s)))
        self._width = len(s)
        sys.stdout.flush()

    def _erase(self) -> None:
        if self._live and self._width:
            sys.stdout.write("\r" + " " * self._width + "\r")

    def line(self, text: str) -> None:
        with self._lock:
            self._erase()
            print(text)
            self._draw()

    def tick(self, status: str) -> None:
        with self._lock:
            if status == "ok":
                self.ok += 1
            elif status == "invalid":
                self.invalid += 1
            else:
                self.failed += 1

    def __enter__(self):
        if self._live:
            self._thread = threading.Thread(target=self._refresh, daemon=True)
            self._thread.start()
        return self

    def _refresh(self) -> None:
        while not self._stop.wait(1.0):
            with self._lock:
                self._draw()

    def __exit__(self, *exc):
        """Leave the FINAL status line on screen as the run summary."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        with self._lock:
            self._draw()          # final values
            if self._live:
                sys.stdout.write("\n")
            else:
                print(self._status())   # non-TTY: the one and only progress line
            sys.stdout.flush()
            self._width = 0
        return False


def main():
   
    all_pdfs = [b for b in storage_client.list_blobs(BUCKET, prefix=PDF_PREFIX)
                if b.name.lower().endswith(".pdf")]
  
    all_pdfs.sort(key=lambda b: _load_num(b.name), reverse=NEWEST_FIRST)
    skip_multi = find_multi_pdf_loads(all_pdfs)
    done = already_done()                        
    print(f"Connected to {PROJECT_ID}--{BUCKET}/{PDF_PREFIX}\n in Region {LOCATION}\n"
          f"{len(all_pdfs)} PDFs found; {len(done)} docs already parsed;\n "
          f"{len(skip_multi)} multi-PDF loads skipped.\n"
          f"Limit = {LIMIT if LIMIT is not None else 'None'}, Max Workers = {MAX_WORKERS} \n"
          f"Writing extractions to {BUCKET}/{JSON_PREFIX}")

    # Build the work list 
    work = []
    for b in all_pdfs:
        load_id = load_id_from_pdf(b.name)
        if load_id in done or load_id in skip_multi:
            continue
        work.append(b)
        if LIMIT is not None and len(work) >= LIMIT:
            break

    print(f"Processing {len(work)} loads with {MAX_WORKERS} concurrent workers...")
    with Progress(len(work)) as prog, ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_blob, b): b for b in work}
        for fut in as_completed(futures):
            b = futures[fut]
            try:
                status, load_id, detail = fut.result()
            except Exception as e:                   # noqa: BLE001 — a worker crashed unexpectedly
                prog.tick("failed")
                prog.line(f"[FAIL] {load_id_from_pdf(b.name)}: worker crashed: {e}")
                continue
            prog.tick(status)
            if status == "ok":
                prog.line(f"[ok] {load_id:>8}  {detail}")
            elif status == "invalid":
                prog.line(f"[skip invalid] {load_id}: {detail}")
            else:
                prog.line(f"[FAIL] {load_id}: {detail}")
        ok = prog.ok

    
    print_cost_summary(ok)


if __name__ == "__main__":
    main()
