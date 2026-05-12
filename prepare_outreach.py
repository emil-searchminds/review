"""
Run locally in the morning.

Walks every results/batch-*.jsonl that the GitHub Actions chain has committed,
filters to companies with recent 1-star reviews, joins emails from
companies-private.csv, and writes:

  outreach.csv                — one row per alert company (ready to mail merge)
  outreach.jsonl              — same data but with full review text per row

Both files are gitignored. The CSV includes:
  name, email, place_id, rating, review_count, alerts, latest_review_age_weeks,
  worst_review_text, maps_url
"""

import csv
import json
from pathlib import Path

RESULTS_DIR = Path("results")
PRIVATE_CSV = Path("companies-private.csv")
OUT_CSV = Path("outreach.csv")
OUT_JSONL = Path("outreach.jsonl")


def main() -> None:
    if not PRIVATE_CSV.exists():
        raise SystemExit(f"missing {PRIVATE_CSV} (the emailed CSV, kept local only)")

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

    alerts: list[dict] = []
    total_scraped = 0
    batches_seen = []

    for bf in batch_files:
        batches_seen.append(bf.name)
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
            pid = obj.get("place_id", "")
            email = emails.get(pid, "")
            if not email:
                continue  # no email = nothing to send
            worst = min(reviews, key=lambda r: r.get("date_weeks", 999))
            alerts.append({
                "name": obj.get("company_name", ""),
                "email": email,
                "place_id": pid,
                "rating": obj.get("rating", 0),
                "review_count": obj.get("review_count", 0),
                "alerts": len(reviews),
                "latest_review_age_weeks": worst.get("date_weeks", 0),
                "worst_review_text": (worst.get("text") or "").replace("\n", " ").strip(),
                "maps_url": obj.get("maps_url", ""),
                "_reviews": reviews,
            })

    alerts.sort(key=lambda a: (a["latest_review_age_weeks"], -a["alerts"]))

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "name", "email", "place_id", "rating", "review_count",
            "alerts", "latest_review_age_weeks", "worst_review_text", "maps_url",
        ])
        for a in alerts:
            writer.writerow([
                a["name"], a["email"], a["place_id"], a["rating"], a["review_count"],
                a["alerts"], a["latest_review_age_weeks"], a["worst_review_text"], a["maps_url"],
            ])

    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for a in alerts:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")

    print(f"Batches consolidated : {len(batches_seen)} ({', '.join(batches_seen)})")
    print(f"Total places scraped : {total_scraped:,}")
    print(f"Alert companies      : {len(alerts):,} (with email)")
    print(f"Wrote                : {OUT_CSV} and {OUT_JSONL}")


if __name__ == "__main__":
    main()
