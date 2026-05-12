"""
Merge all results/chunk-*.jsonl files (downloaded as workflow artifacts)
into a single output file, deduplicated by place_id (last writer wins),
then delete the chunk files.

Output path is provided via --out (default: results/results.jsonl).
"""

import argparse
import json
from pathlib import Path

RESULTS_DIR = Path("results")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=RESULTS_DIR / "results.jsonl")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    merged: dict[str, dict] = {}
    if args.out.exists():
        for line in args.out.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                pid = obj.get("place_id")
                if pid:
                    merged[pid] = obj
            except json.JSONDecodeError:
                pass

    chunk_files = sorted(RESULTS_DIR.glob("chunk-*.jsonl"))
    new_count = 0
    for cf in chunk_files:
        for line in cf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                pid = obj.get("place_id")
                if pid:
                    merged[pid] = obj
                    new_count += 1
            except json.JSONDecodeError:
                pass

    with args.out.open("w", encoding="utf-8") as f:
        for pid in sorted(merged):
            f.write(json.dumps(merged[pid], ensure_ascii=False) + "\n")

    for cf in chunk_files:
        cf.unlink()

    alerts = sum(1 for r in merged.values() if r.get("one_star_reviews"))
    errors = sum(1 for r in merged.values() if r.get("error"))
    print(
        f"merged {len(chunk_files)} chunks "
        f"({new_count} records this run, {len(merged)} total in {args.out.name})  |  "
        f"alerts: {alerts}  errors: {errors}"
    )


if __name__ == "__main__":
    main()
