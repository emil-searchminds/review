"""
One worker = one slice of companies.csv.

Run as:
    python worker.py --chunk-index 0 --chunk-count 10 --max-age-weeks 4

Reads companies.csv, picks rows where (row_index % chunk_count == chunk_index),
scrapes them via scraper.monitor_batch, and writes results/chunk-NNN.jsonl
incrementally — each finished place is appended immediately, so a late crash
(e.g. Chromium dying during browser.close at the end of the run) doesn't lose
the whole batch.
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
    p.add_argument(
        "--batch-label",
        type=str,
        default=None,
        help="If set, skip place_ids that already have a non-error result in "
             "results/batch-<label>.jsonl (re-run only failures).",
    )
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


def already_done(batch_label: str | None) -> set[str]:
    """place_ids that have a successful (non-error) result in the existing batch file."""
    if not batch_label:
        return set()
    batch_file = Path(f"results/batch-{batch_label}.jsonl")
    if not batch_file.exists():
        return set()
    done: set[str] = set()
    for line in batch_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        pid = obj.get("place_id")
        if pid and not obj.get("error"):
            done.add(pid)
    return done


async def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    companies = load_slice(args.companies, args.chunk_index, args.chunk_count)
    done_pids = already_done(args.batch_label)
    place_ids = [pid for pid in companies.keys() if pid not in done_pids]
    total = len(place_ids)
    skipped = len(companies) - total

    out_file = args.out_dir / f"chunk-{args.chunk_index:03d}.jsonl"
    out_file.write_text("", encoding="utf-8")  # truncate / create

    print(
        f"chunk {args.chunk_index}/{args.chunk_count}: "
        f"{total} to scrape  (+{skipped} already done in batch-{args.batch_label})"
    )

    if not place_ids:
        return

    write_lock = asyncio.Lock()
    fh = out_file.open("a", encoding="utf-8")

    counters = {"done": 0, "alerts": 0, "errors": 0}

    async def on_result(pid: str, r: dict) -> None:
        company = companies.get(pid, {})
        record = {
            "place_id": pid,
            "company_name": company.get("name", ""),
            "maps_url": f"https://www.google.com/maps/place/?q=place_id:{pid}",
            **r,
        }
        async with write_lock:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
        counters["done"] += 1
        if r.get("one_star_reviews"):
            counters["alerts"] += 1
        if r.get("error"):
            counters["errors"] += 1
        if counters["done"] % 100 == 0 or counters["done"] == total:
            print(
                f"  {counters['done']}/{total}  "
                f"alerts: {counters['alerts']}  errors: {counters['errors']}",
                flush=True,
            )

    try:
        await monitor_batch(
            place_ids,
            workers=args.workers,
            max_age_weeks=args.max_age_weeks,
            on_result=on_result,
        )
    finally:
        fh.close()

    print(
        f"done: {counters['done']}/{total}  "
        f"alerts: {counters['alerts']}  errors: {counters['errors']}  "
        f"-> {out_file}"
    )


if __name__ == "__main__":
    asyncio.run(main())
