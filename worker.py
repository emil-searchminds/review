"""
One worker = one slice of companies.csv.

Run as:
    python worker.py --chunk-index 0 --chunk-count 10 --max-age-weeks 4

Reads companies.csv, picks rows where (row_index % chunk_count == chunk_index),
scrapes them via scraper.monitor_batch, and writes results/chunk-NN.jsonl.
"""

import argparse
import asyncio
import csv
import json
from pathlib import Path

from scraper.google_maps import monitor_batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--chunk-index", type=int, required=True)
    p.add_argument("--chunk-count", type=int, required=True)
    p.add_argument("--max-age-weeks", type=int, default=4)
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--companies", type=Path, default=Path("companies.csv"))
    p.add_argument("--out-dir", type=Path, default=Path("results"))
    return p.parse_args()


def load_slice(csv_path: Path, chunk_index: int, chunk_count: int) -> dict[str, dict]:
    companies: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if i % chunk_count != chunk_index:
                continue
            pid = (row.get("place_id") or "").strip()
            if pid:
                companies[pid] = row
    return companies


async def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    companies = load_slice(args.companies, args.chunk_index, args.chunk_count)
    place_ids = list(companies.keys())

    print(f"chunk {args.chunk_index}/{args.chunk_count}: {len(place_ids)} places")

    if not place_ids:
        out_file = args.out_dir / f"chunk-{args.chunk_index:03d}.jsonl"
        out_file.write_text("", encoding="utf-8")
        return

    results = await monitor_batch(
        place_ids,
        workers=args.workers,
        max_age_weeks=args.max_age_weeks,
    )

    out_file = args.out_dir / f"chunk-{args.chunk_index:03d}.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for pid, r in results.items():
            company = companies.get(pid, {})
            record = {
                "place_id": pid,
                "company_name": company.get("name", ""),
                "email": company.get("email", "") or company.get("email_1", ""),
                "maps_url": f"https://www.google.com/maps/place/?q=place_id:{pid}",
                **r,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    alerts = sum(1 for r in results.values() if r.get("one_star_reviews"))
    errors = sum(1 for r in results.values() if r.get("error"))
    print(f"wrote {out_file}  |  alerts: {alerts}  errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
