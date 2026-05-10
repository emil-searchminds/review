"""
Local-only: re-attach emails (from companies-private.csv) to results/results.jsonl.

Reads:  companies-private.csv  (gitignored — full data with emails)
        results/results.jsonl  (output of the GitHub Actions scrape)
Writes: results/results-with-emails.jsonl  (gitignored)

Run after pulling the latest scrape results.
"""

import csv
import json
from pathlib import Path

PRIVATE_CSV = Path("companies-private.csv")
RESULTS_IN = Path("results/results.jsonl")
RESULTS_OUT = Path("results/results-with-emails.jsonl")


def main() -> None:
    if not PRIVATE_CSV.exists():
        raise SystemExit(f"missing {PRIVATE_CSV} — keep your emailed CSV here locally")
    if not RESULTS_IN.exists():
        raise SystemExit(f"missing {RESULTS_IN} — pull the latest scrape first")

    emails: dict[str, str] = {}
    with PRIVATE_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = (row.get("place_id") or "").strip()
            email = (row.get("email") or row.get("email_1") or "").strip()
            if pid and email:
                emails[pid] = email

    enriched = 0
    total = 0
    with RESULTS_IN.open(encoding="utf-8") as fin, RESULTS_OUT.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            total += 1
            email = emails.get(obj.get("place_id", ""))
            if email:
                obj["email"] = email
                enriched += 1
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"wrote {RESULTS_OUT}  |  {enriched}/{total} records got an email")


if __name__ == "__main__":
    main()
