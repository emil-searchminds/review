# Distributed Review Scraper

Scrapes Google Maps for recent 1-star reviews across many companies, in parallel,
on free GitHub Actions runners. Each runner gets a fresh IP from the Azure pool —
natural rotation, zero proxy setup.

## How it works

1. You commit `companies.csv` (`name,email,place_id`).
2. You trigger the **Scrape** workflow with `worker_count = N`.
3. GitHub fans out N parallel jobs. Each job scrapes a 1/N slice (`row_index % N == job_index`).
4. Each job uploads its results as a workflow artifact.
5. A final `aggregate` job downloads all artifacts, merges them into `results/results.jsonl` (deduped by `place_id`), and commits it back to the repo.

## Run it

GitHub UI → **Actions** → **Scrape** → **Run workflow** → set `worker_count` → Run.

Or via CLI:

```bash
gh workflow run scrape.yml -f worker_count=10 -f max_age_weeks=4
```

## Tuning

- **`worker_count`** — number of parallel runners. 10–20 is a good start.
- **`workers_per_runner`** — concurrent browser contexts per runner. The original local pipeline used 20; on a 2-core GH runner, 10 is safer.
- **`max_age_weeks`** — only collect 1-star reviews newer than this. Smaller = faster (early-exit triggers sooner).

Throughput ≈ `worker_count × workers_per_runner` places in flight.

## Output

`results/results.jsonl` — one JSON object per place:

```json
{
  "place_id": "ChIJ...",
  "company_name": "...",
  "email": "...",
  "maps_url": "https://www.google.com/maps/place/?q=place_id:...",
  "rating": 4.6,
  "review_count": 132,
  "one_star_reviews": [
    {"reviewer_name": "...", "rating": 1, "text": "...", "date": "2 weeks ago", "date_weeks": 2}
  ]
}
```

## Local dev

```bash
pip install -r requirements.txt
python -m playwright install chromium
python worker.py --chunk-index 0 --chunk-count 1
python aggregate.py
```
