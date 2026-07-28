# Build Guide — from zero

Follow these phases in order. Every command is copy-paste. Run them in **PowerShell**
(search "PowerShell" in the Start menu). You do **not** need to understand cloud
computing to complete this — just follow along.

> **Values you'll reuse everywhere.** Write them down now:
> - **PROJECT_ID** — your Google Cloud project id (see Phase 1 to find it)
> - **BUCKET** — a globally-unique storage name, e.g. `linic-freight-docs`
> - **REGION** — `us-east1` for Vertex/Gemini; bucket + BigQuery in US
> - **DOCAI_LOCATION** — `us` (Document AI's US multi-region; set in `extract.py`)
> - **OCR_PROCESSOR_ID** — created in Phase 2

---

## Phase 0 — Install the three tools (~20 min)

1. **Google Cloud CLI** ("gcloud") — download the installer:
   https://cloud.google.com/sdk/docs/install → run it → at the end let it run `gcloud init`.
2. **Python 3.12** — https://www.python.org/downloads/ → during install **check "Add Python to PATH"**.
3. **rclone** — https://rclone.org/downloads/ → download the Windows zip, unzip, and copy
   `rclone.exe` into a folder on your PATH (e.g. `C:\Windows\System32`).

Verify all three (each should print a version):
```powershell
gcloud --version
python --version
rclone version
```

---

## Phase 1 — Sign in and pick your project (~5 min)

```powershell
gcloud auth login
gcloud auth application-default login
gcloud projects list
```
The last command prints your projects. Copy the **PROJECT_ID** you want, then:
```powershell
gcloud config set project YOUR_PROJECT_ID
```

Turn on the services this project uses (safe to run; just enables features):
```powershell
gcloud services enable aiplatform.googleapis.com bigquery.googleapis.com storage.googleapis.com documentai.googleapis.com
```

---

## Phase 2 — Create the bucket, database, and OCR processor (~10 min)

```powershell
# A bucket to hold PDFs, JSON, and exports (name must be globally unique)
gcloud storage buckets create gs://YOUR_BUCKET --location=US

# A BigQuery dataset named "freight"
bq --location=US mk -d YOUR_PROJECT_ID:freight
```

**Create the tables:** open `schema\bigquery_setup.sql`, use Find & Replace to change every
`PROJECT` to your real project id, then run **Section A** of that file in the BigQuery
console (https://console.cloud.google.com/bigquery → paste → Run). Leave Section B for later.

**Create the OCR processor:** in the Console → **Document AI** → **Processor Gallery** →
**Document OCR** → **Create processor** → region **`us`** → name it → **Create**. On the
processor page, copy the **Processor ID** (a long hex string) — you'll paste it into
`extract.py` as `OCR_PROCESSOR_ID`.

---

## Phase 3 — Copy the PDFs from Drive into Cloud Storage (~time varies)

**a) Connect rclone to Google Drive.** Run `rclone config` and answer the prompts:
- `n` (new remote) → name it **`gdrive`**
- storage type: **`drive`**
- client_id / client_secret: press Enter (blank)
- scope: choose **`1`** (full access)
- press Enter through the rest → when it asks to authenticate, choose **Yes (auto)** — a
  browser opens; sign in with the Google account that has the documents.
- "Configure this as a Shared Drive?" → **Yes** if the files are in a company Shared Drive,
  otherwise **No**.

**b) Connect rclone to Cloud Storage.** Run `rclone config` again:
- `n` → name it **`gcs`**
- storage type: **`google cloud storage`**
- project_number: your project id
- for auth, choose **application default credentials** (you already ran the login in Phase 1)
- accept the defaults for the rest.

**c) Copy, preserving the folder-per-load structure.** Replace the Drive folder name:
```powershell
rclone copy "gdrive:YOUR_DRIVE_FOLDER" "gcs:YOUR_BUCKET/raw/loads" `
  --progress --transfers=8 --checkers=16 --tpslimit=10 --retries=10 --low-level-retries=20
```
This is **resumable** — if it stops, run the exact same command again and it continues.

**d) Verify the count (~19,000 PDFs):**
```powershell
(gcloud storage ls -r "gs://YOUR_BUCKET/raw/loads/**" | Select-String "\.pdf$").Count
```

---

## Phase 4 — The PILOT: 100 documents (~30 min) — do NOT skip this

1. Install the Python libraries (`requirements.txt` is in the project root):
   ```powershell
   cd C:\Users\markl\OneDrive\Desktop\myDocDashboard
   python -m pip install -r requirements.txt
   ```
2. Open `pipeline\extract.py` and edit the values at the top: `PROJECT_ID`, `LOCATION`
   (Vertex region, e.g. `us-east1`), `BUCKET`, `DOCAI_LOCATION` (`us`), `OCR_PROCESSOR_ID`
   (from Phase 2), and leave `LIMIT = 100`. Make sure `PDF_PREFIX` matches where you copied
   the PDFs in Phase 3 (e.g. `raw/loads/`).
3. Run it:
   ```powershell
   cd pipeline
   python extract.py
   ```
   It OCRs + extracts 100 loads and writes JSON to `gs://YOUR_BUCKET/json/`.
4. Load those results and check quality:
   - In the BigQuery console, run **Section B** of `schema\bigquery_setup.sql` after first
     running the `bq load` command shown in that file's comments (points at `json/*.json`).
   - Then run:
     ```sql
     SELECT doc_type, COUNT(*) docs,
            ROUND(AVG(confidence),2)     AS avg_gemini_conf,
            ROUND(AVG(ocr_confidence),2) AS avg_ocr_conf,
            SUM(CAST(needs_review AS INT64)) AS to_review
     FROM `YOUR_PROJECT.freight.documents` GROUP BY doc_type;
     ```
5. **Spot-check**: open 5–10 of the original PDFs and compare the extracted fields.
   - If accuracy is good → go to Phase 5.
   - If specific fields are weak → tell me which; we tune the prompt, the schema, or the
     scan resolution.

---

## Phase 5 — The full run (19,000)

1. In `pipeline\extract.py`, set `LIMIT = None`.
2. Run it again:
   ```powershell
   python extract.py
   ```
   It skips the 100 already done and processes the rest. If your PC sleeps or the run stops,
   just run the same command again — it resumes. (Want it to run unattended on a server
   instead of your PC? Ask me and I'll add the ~10-minute Compute Engine VM steps.)
3. Check for problems (two log buckets):
   ```powershell
   gcloud storage ls "gs://YOUR_BUCKET/logs/failed/"  2>$null | Measure-Object -Line   # OCR/Gemini errors
   gcloud storage ls "gs://YOUR_BUCKET/logs/invalid/" 2>$null | Measure-Object -Line   # broken PDFs
   ```
   Re-running `extract.py` retries anything without an output file.

---

## Phase 6 — Load everything and export for ML

1. Load all JSON into staging and flatten (BigQuery console): re-run the `bq load` command
   and **Section B** of `schema\bigquery_setup.sql`. It's safe to re-run — it rebuilds the
   `documents` and `loads` tables from scratch.
2. Sanity check:
   ```sql
   SELECT COUNT(*) loads, SUM(doc_count) documents, SUM(docs_needing_review) to_review
   FROM `YOUR_PROJECT.freight.loads`;
   ```
3. Export the training data:
   ```powershell
   bq extract --destination_format=PARQUET `
     YOUR_PROJECT_ID:freight.loads "gs://YOUR_BUCKET/exports/loads-*.parquet"
   ```

Done — you now have clean, structured, ML-ready freight data, with a `needs_review` flag
telling you exactly which rows a human should double-check. The Looker Studio dashboard
connects straight to the `freight.loads` table when you're ready for it.
