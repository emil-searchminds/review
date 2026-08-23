"""
Run locally. Builds the file the original outreach.py mailer expects:

  data/results.jsonl   — JSONL, one company per line, with fields:
                         place_id, company_name, email, maps_url,
                         market, rating, review_count, one_star_reviews

Only includes places that have BOTH a recent 1-star review AND an email
on file (outreach.py filters on these anyway, so pre-filtering keeps the
file lean).

Also writes outreach.csv for human review — same data, flat.

Sorted by latest_review_age_weeks ascending, so when outreach.py picks
the first N pending it contacts the freshest alerts first.
"""

import csv
import json
from pathlib import Path

RESULTS_DIR = Path("results")
PRIVATE_CSV = Path("companies-private.csv")
OUT_JSONL = Path("data/results.jsonl")
OUT_CSV = Path("outreach.csv")


def main() -> None:
    if not PRIVATE_CSV.exists():
        raise SystemExit(f"missing {PRIVATE_CSV} (local-only emails file)")

    emails: dict[str, str] = {}
    with PRIVATE_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = (row.get("place_id") or "").strip()
            email = (row.get("email") or row.get("email_1") or "").strip()
            if pid and email:
                emails[pid] = email

    batch_files = sorted(RESULTS_DIR.glob("batch-*.jsonl"))
    if not batch_files:
        raise SystemExit("no results/batch-*.jsonl found — has any batch finished?")

    OUT_JSONL.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    total_scraped = 0
    alerts_total = 0

    for bf in batch_files:
        for line in bf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            total_scraped += 1
            reviews = obj.get("one_star_reviews") or []
            if not reviews:
                continue
            alerts_total += 1
            pid = obj.get("place_id", "")
            email = emails.get(pid, "")
            if not email:
                continue
            worst_age = min((r.get("date_weeks", 999) for r in reviews), default=999)
            rows.append({
                "place_id": pid,
                "company_name": obj.get("company_name", ""),
                "email": email,
                "maps_url": obj.get("maps_url", ""),
                "market": "",
                "rating": obj.get("rating", 0),
                "review_count": obj.get("review_count", 0),
                "one_star_reviews": reviews,
                "_worst_age": worst_age,
            })

    rows.sort(key=lambda r: (r["_worst_age"], -len(r["one_star_reviews"])))

    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for r in rows:
            payload = {k: v for k, v in r.items() if not k.startswith("_")}
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "company_name", "email", "place_id", "rating", "review_count",
            "alerts", "latest_review_age_weeks", "worst_review_text", "maps_url",
        ])
        for r in rows:
            worst = min(r["one_star_reviews"], key=lambda x: x.get("date_weeks", 999))
            w.writerow([
                r["company_name"], r["email"], r["place_id"],
                r["rating"], r["review_count"], len(r["one_star_reviews"]),
                r["_worst_age"],
                (worst.get("text") or "").replace("\n", " ").strip(),
                r["maps_url"],
            ])

    print(f"Batches consolidated : {len(batch_files)} ({', '.join(b.stem for b in batch_files)})")
    print(f"Total places scanned : {total_scraped:,}")
    print(f"Alert companies      : {alerts_total:,}")
    print(f"With email (output)  : {len(rows):,}")
    print(f"Wrote                : {OUT_JSONL} and {OUT_CSV}")
    print()
    print("Drop data/results.jsonl into the original review_tracker repo "
          "and run its outreach.py.")


if __name__ == "__main__":
    main()
