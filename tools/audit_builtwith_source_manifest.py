from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


ROOT = Path("/Users/laurencedeer/Desktop/BuiltWith")
RAW_DIR = ROOT / "BuiltWith Exports"
MANIFEST_PATH = ROOT / "config" / "builtwith_source_manifest.csv"


def main() -> None:
    manifest_rows = []
    with MANIFEST_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        manifest_rows = list(csv.DictReader(handle))

    counts = Counter(row["report_type"] if row["include"].lower() == "true" else "quarantined" for row in manifest_rows)
    print("BuiltWith source manifest audit")
    print(f"Manifest rows: {len(manifest_rows)}")
    for key, count in sorted(counts.items()):
        print(f"{key}: {count}")
    print()
    print("relative_path\tprimary_cms\treport_type\tconfidence\tinclude\trows\tdominance\tnotes")
    for row in manifest_rows:
        source_path = RAW_DIR / row["relative_path"]
        exists = "yes" if source_path.exists() else "missing"
        print(
            "\t".join(
                [
                    row["relative_path"],
                    row["primary_cms"],
                    row["report_type"],
                    row["confidence"],
                    row["include"],
                    row["row_count"],
                    row["dominance_percent"],
                    exists,
                    row["notes"],
                ]
            )
        )


if __name__ == "__main__":
    main()
