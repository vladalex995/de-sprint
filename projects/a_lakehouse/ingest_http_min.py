# projects/a_lakehouse/ingest_http_min.py
# Goal: download one file over HTTP and land it in the raw/ area (as-is),
# and append a small audit log (manifest) so runs are traceable and idempotent (safe to re-run).

import logging               # Logging = timestamped messages you can read later
from pathlib import Path     # Path objects make file/folder paths easy and safe
from datetime import date, datetime  # date for folder names; datetime for precise timestamps
import requests              # HTTP client library (lets Python download from URLs)  <-- pip install requests
import csv                   # To write a tiny CSV "manifest" log
import hashlib               # SHA-256 checksum = file fingerprint
import argparse              # CLI argument parser (enables --url, --id, etc.)
import sys                   # sys.exit(...) for clean program exit

# ── Project folders (we place this file directly in a_lakehouse/) ─────────────

HERE = Path(__file__).resolve()         # Full path to this file
BASE = HERE.parent                      # a_lakehouse/ (because this file sits there)
RAW = BASE / "data" / "raw"             # raw landing zone (no transformation)
MANIFEST = RAW / "_ingest_log.csv"      # small audit log we append to on each run

# ── Small helpers ─────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> None:
    """
    Configure logging once for the whole script.
    level: "DEBUG" | "INFO" | "WARNING" | "ERROR"
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),  # map text to numeric level
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",  # include timestamp + level + logger name
    )

def today_str() -> str:
    """Return today's date as 'YYYY-MM-DD' for folder naming."""
    return date.today().isoformat()

def now_ts() -> str:
    """Return a human timestamp for the manifest, e.g., '2025-08-25T13:45:00'."""
    return datetime.now().isoformat(timespec="seconds")

def ensure_dir(p: Path) -> None:
    """Create folder p (and parents) if missing (no error if it exists)."""
    p.mkdir(parents=True, exist_ok=True)

def sha256_bytes(b: bytes) -> str:
    """Compute SHA-256 checksum (fingerprint) of some bytes for integrity tracking."""
    h = hashlib.sha256()  # create a new hasher
    h.update(b)           # feed all bytes
    return h.hexdigest()  # 64-char hex string (stable fingerprint)

def append_manifest_row(row: dict) -> None:
    """
    Append one line to raw/_ingest_log.csv.
    If the file doesn't exist yet, write a header first.
    """
    ensure_dir(RAW)                                  # make sure raw/ exists
    new_file = not MANIFEST.exists()                 # decide whether to write the header
    with MANIFEST.open("a", newline="", encoding="utf-8") as f:  # append mode
        fieldnames = [                               # fixed column order for the manifest
            "run_ts", "source_type", "source_id", "input_url",
            "out_path", "rows", "bytes", "checksum", "status", "note"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()                     # first line = header row
        writer.writerow(row)                         # data row

# ── Core ingestion (HTTP file → raw/) ────────────────────────────────────────

def ingest_http_file(
    url: str,                    # web address to download (HTTP GET request)
    source_id: str,              # short label for folder naming (e.g., "tips")
    run_date: str | None = None, # date subfolder; defaults to today if None
    force: bool = False,         # if True, re-download even if output already exists
    log_level: str = "INFO",     # "DEBUG" for more detail; "INFO" normally
) -> Path:
    """
    Download a file and land it (as-is) into:
      raw/files/{source_id}/{date}/<filename>
    Then append a manifest line (audit trail).
    """
    setup_logging(log_level)                # turn on logging
    log = logging.getLogger("ingest.http")  # named logger for this function

    d = run_date or today_str()             # pick date folder (argument or today)
    out_dir = RAW / "files" / source_id / d # e.g., raw/files/tips/2025-08-25/
    ensure_dir(out_dir)                     # create folders (safe if they already exist)

    filename = url.split("/")[-1] or "download.bin"  # last part of URL becomes filename
    out_path = out_dir / filename                     # final file path on disk

    # Idempotency: if the output already exists and user didn't pass --force, skip.
    if out_path.exists() and not force:
        log.info("skip existing file: %s", out_path)
        append_manifest_row({
            "run_ts": now_ts(),
            "source_type": "http-file",
            "source_id": source_id,
            "input_url": url,
            "out_path": str(out_path),
            "rows": "",                              # unknown for generic files
            "bytes": out_path.stat().st_size,        # file size in bytes
            "checksum": "",                          # skip calc when skipping
            "status": "SKIP",
            "note": "already exists",
        })
        return out_path

    # Do the HTTP GET request (ask the server for the file). timeout prevents hanging forever.
    r = requests.get(url, timeout=30)
    r.raise_for_status()                # if server replied with error code, raise an exception

    data = r.content                    # raw bytes of the response body
    out_path.write_bytes(data)          # save bytes to disk (binary safe)
    checksum = sha256_bytes(data)       # compute fingerprint for integrity/audit

    # Log success and append one manifest row describing what we ingested.
    log.info("downloaded %s → %s bytes=%d", url, out_path, len(data))
    append_manifest_row({
        "run_ts": now_ts(),
        "source_type": "http-file",
        "source_id": source_id,
        "input_url": url,
        "out_path": str(out_path),
        "rows": "",                     # not applicable for raw files
        "bytes": len(data),
        "checksum": checksum,
        "status": "OK",
        "note": "",
    })
    return out_path                     # return the file path for convenience

# ── Command-line interface (so schedulers/people can pass parameters) ────────

def build_parser() -> argparse.ArgumentParser:
    """Define CLI arguments."""
    p = argparse.ArgumentParser(description="HTTP file → raw/ ingest (idempotent)")
    p.add_argument("--url", required=True, help="HTTP/HTTPS address of the file")
    p.add_argument("--id", required=True, help="short source id, e.g., 'tips'")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (defaults to today)")
    p.add_argument("--force", action="store_true", help="re-download even if output exists")
    p.add_argument("--log", default="INFO", help="log level: DEBUG/INFO/WARNING/ERROR")
    return p

def main(argv: list[str]) -> int:
    """Parse args → run ingest → print output path → exit with 0 on success."""
    parser = build_parser()          # make the parser
    args = parser.parse_args(argv)   # parse incoming CLI arguments
    out = ingest_http_file(          # run the core function
        url=args.url,
        source_id=args.id,
        run_date=args.date,
        force=args.force,
        log_level=args.log,
    )
    print(out)                       # print where we wrote the file (useful in logs)
    return 0                         # 0 = success

if __name__ == "__main__":          # only runs when you execute this file directly
    sys.exit(main(sys.argv[1:]))    # pass CLI args (excluding program name) and exit cleanly
