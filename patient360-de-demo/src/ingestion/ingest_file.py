"""File ingestion: land CSV/JSON files into Bronze (raw, as-is + load metadata).

Demo run (local):  python src/ingestion/ingest_file.py
Azure run:        ADF Copy activity to the Bronze container, or Databricks
                  Auto Loader (cloudFiles) -- same Bronze contract.
"""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
LANDING = PROJECT / "data" / "file"
BRONZE = PROJECT / "data" / "bronze"  # local stand-in for the Bronze container


def count_rows(src: Path) -> int:
    """Record count: data rows for CSV, array length for JSON."""
    if src.suffix == ".csv":
        return sum(1 for _ in open(src, encoding="utf-8")) - 1
    if src.suffix == ".json":
        return len(json.loads(src.read_text(encoding="utf-8")))
    return sum(1 for _ in open(src, encoding="utf-8"))


def ingest_file(src: Path) -> Path:
    """Copy a landing file to Bronze with a batch stamp. No transformation."""
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest_dir = BRONZE / src.stem / f"batch_id={batch_id}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copy2(src, dest)
    meta = {
        "source_file": src.name,
        "batch_id": batch_id,
        "load_timestamp": datetime.now(timezone.utc).isoformat(),
        "rows": count_rows(src),
    }
    (dest_dir / "_metadata.json").write_text(json.dumps(meta, indent=2))
    print(f"landed {src.name} -> {dest} ({meta['rows']} rows)")
    return dest


if __name__ == "__main__":
    for name in ("lab_results.csv", "claims.json"):
        ingest_file(LANDING / name)
