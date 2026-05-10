#!/usr/bin/env python3
"""
Google Maps review scraper — async Playwright implementation.

Optimisation notes (v3 — batch-speed focus):
  • _http_get_meta now returns the canonical URL (captured from the HTTP redirect),
    so _scrape_page can skip the first browser navigation entirely when the HTTP
    path succeeds.  One navigation per place instead of two.
  • _scroll_all_reviews: 8 000 px/step (was 2 000), 400 ms wait (was 800 ms),
    2 stable iterations (was 3).  Worst-case scroll time drops from ~64 s to ~16 s.
  • _open_reviews_list is now a fallback — only called when [data-review-id] does
    not appear within 12 s of landing on the reviews URL.
  • Removed both unconditional wait_for_selector('[role="main"]') calls and the
    2 s pre-scroll wait_for_timeout.
  • get_reviews_async fires the HTTP meta request concurrently while Playwright is
    loading the page (asyncio.gather).
  • scrape_batch: one shared browser, per-worker BrowserContext pool, asyncio
    Semaphore concurrency cap, per-place error isolation, optional progress callback.
"""

import re
import json
import asyncio
import concurrent.futures
import httpx
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# SOCS=CAI... grants EU consent so Google never redirects to consent.google.com.
_CONSENT_COOKIES_PW = [
    {
        "name": "SOCS",
        "value": "CAISHAgCEhJnd3NfMjAyMzA4MTUtMF9SQzIaAmVuIAEaBgiA_LSnBg",
        "domain": ".google.com",
        "path": "/",
    },
    {
        "name": "CONSENT",
        "value": "YES+cb.20230107-08-p1.en+FX+111",
        "domain": ".google.com",
        "path": "/",
    },
]
_HTTP_COOKIES = {c["name"]: c["value"] for c in _CONSENT_COOKIES_PW}
_HEADERS = {"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"}

# ─────────────────────────────────────────────────────────────────────────────
# JS snippets
# ─────────────────────────────────────────────────────────────────────────────

# Count unique top-level reviews (those containing a name element).
_COUNT_JS = """
() => {
  const seen = new Set();
  for (const el of document.querySelectorAll('[data-review-id], div.jftiEf')) {
    const hasName = el.querySelector('.d4r55, a[href*="/maps/contrib/"], [class*="fontHeadlineSmall"]');
    if (!hasName) continue;
    const rid = el.getAttribute('data-review-id');
    const key = rid || hasName.innerText.trim().split('\\n')[0];
    if (key) seen.add(key);
  }
  return seen.size;
}
"""

# Scroll the first overflow:auto/scroll element whose content exceeds its viewport.
# Always jumps to el.scrollHeight (the absolute bottom) so that Google's infinite-
# scroll lazy-loader fires on every call regardless of how tall the panel is.
_SCROLL_JS = """
() => {
  for (const el of document.querySelectorAll('*')) {
    const ov = window.getComputedStyle(el).overflowY;
    if ((ov === 'auto' || ov === 'scroll') && el.scrollHeight > el.clientHeight + 10) {
      el.scrollTop = el.scrollHeight;
      return {cls: el.className.substring(0, 80), st: el.scrollTop, sh: el.scrollHeight};
    }
  }
  return null;
}
"""

# Extract aggregate rating and review count from aria-labels / visible text.
_META_JS = """
() => {
  // Primary: .jANrlb renders "4.8  18 reviews" on the full reviews list page
  const jn = document.querySelector('.jANrlb');
  if (jn) {
    const txt = jn.innerText || '';
    const m = txt.match(/([\d,.]+)[\s\S]*?(\d[\d,]*)\s+review/i);
    if (m) return {rating: parseFloat(m[1].replace(',','.')), count: parseInt(m[2].replace(/,/g,''))};
  }
  // .F7nice — overview page renders "4.8" or "4.8 (18)"
  const fn = document.querySelector('.F7nice');
  if (fn) {
    const txt = fn.innerText || '';
    const rm = txt.match(/([\\d][\\d,.]*)/);
    const cm = txt.match(/\\((\\d[\\d,]*)\\)/);
    const rating = rm ? parseFloat(rm[1].replace(',','.')) : 0;
    const count  = cm ? parseInt(cm[1].replace(/,/g,'')) : 0;
    if (rating) return {rating, count};
  }
  // Scan aria-labels: "4.8 stars" / "18 reviews"  (trailing spaces ok)
  let rating = 0, count = 0;
  for (const el of document.querySelectorAll('[aria-label]')) {
    const l = el.getAttribute('aria-label') || '';
    if (!rating) { const mr = l.match(/([\d.]+)\s+stars?/i);  if (mr) rating = parseFloat(mr[1]); }
    if (!count)  { const mc = l.match(/([\d,]+)\s+reviews?/i); if (mc) count  = parseInt(mc[1].replace(/,/g,'')); }
    if (rating && count) return {rating, count};
  }
  return rating ? {rating, count} : null;
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 1 — Direct HTTP (rating + review_count + canonical_url)
# ─────────────────────────────────────────────────────────────────────────────

def _http_get_meta(place_id: str) -> "dict | None":
    """
    Fetch the place page via plain HTTP and extract:
      • rating        (float)
      • review_count  (int)
      • canonical_url (str)  — the /maps/place/…/data=… URL after redirects

    Returns a dict with those keys, or None on any failure.
    JSON-LD only embeds the first 3 reviews, so we never use it for the full list.
    The canonical_url is used by _scrape_page to skip the first Playwright navigation.
    """
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en&gl=us"
    try:
        with httpx.Client(
            headers=_HEADERS,
            cookies=_HTTP_COOKIES,
            follow_redirects=True,
            timeout=20,
        ) as c:
            r = c.get(url)
            final_url = str(r.url)
            if "consent.google.com" in final_url:
                return None
            html = r.text

        meta: dict = {"canonical_url": final_url}

        for raw in re.findall(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        ):
            try:
                d = json.loads(raw.strip())
                if "aggregateRating" not in d:
                    continue
                ar = d["aggregateRating"]
                rating = float(ar.get("ratingValue", 0))
                count = int(ar.get("reviewCount", 0))
                if rating and count:
                    meta["rating"] = rating
                    meta["review_count"] = count
                    return meta
            except Exception:
                pass

        # Return at least the canonical URL even if rating/count couldn't be parsed.
        return meta
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 2 — Async Playwright helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_reviews_url(canonical_url: str) -> str:
    """
    Append !9m1!1b1 to the /data= segment to open the Reviews tab directly.
    Always injects hl=en&gl=us so the UI is in English.
    """
    if "/data=" not in canonical_url:
        return canonical_url + "?hl=en&gl=us"
    path_part, _, qs = canonical_url.partition("?")
    reviews_path = path_part + "!9m1!1b1"
    if "hl=" not in qs:
        qs = ("hl=en&gl=us&" + qs).rstrip("&") if qs else "hl=en&gl=us"
    return reviews_path + "?" + qs


async def _safe_goto(page, url: str, timeout: int = 60_000) -> None:
    """Navigate, tolerating ERR_ABORTED from redirect chains."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    except Exception:
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass


async def _handle_consent(page) -> None:
    """Click through the EU consent screen if cookies somehow didn't bypass it."""
    if "consent.google.com" not in page.url:
        return
    for btn_text in ["Accept all", "Reject all", "I agree"]:
        try:
            await page.locator(f'button:has-text("{btn_text}")').first.click(timeout=5_000)
            await page.wait_for_url(re.compile(r"google\.com/maps"), timeout=30_000)
            return
        except Exception:
            pass


async def _open_reviews_list(page) -> bool:
    """
    Try to open the full infinite-scroll reviews list by clicking "More reviews".

    Background: the !9m1!1b1 reviews URL only activates the reviews *tab* on the
    overview panel — it shows at most 3 summary reviews.  The full scrollable list
    (.m6QErb) only appears after clicking "More reviews".

    Returns True if the button was found and clicked (full list should now be open),
    False if the button was not found (place probably has ≤3 reviews total).
    """
    try:
        btn = page.locator('button:has-text("More reviews")').first
        if await btn.count() > 0 and await btn.is_visible(timeout=3_000):
            await btn.click(timeout=5_000)
            # Wait for the full list container OR for new review blocks to appear.
            try:
                await page.wait_for_function(
                    "() => document.querySelectorAll('[data-review-id]').length > 3",
                    timeout=10_000,
                )
            except Exception:
                await page.wait_for_timeout(2_000)
            return True
    except Exception:
        pass
    return False


async def _scroll_all_reviews(page) -> None:
    """
    Scroll the reviews list until no new [data-review-id] blocks appear for 3 iterations.
    Jumps to el.scrollHeight on every call so Google's lazy-loader always fires.
    600 ms settle wait balances throughput and reliability.
    """
    prev = 0
    stable = 0
    for _ in range(60):  # guard: 60 iterations is plenty for any real-world review count
        try:
            await page.evaluate(_SCROLL_JS)
        except Exception:
            break
        await page.wait_for_timeout(600)
        try:
            n = await page.evaluate(_COUNT_JS)
        except Exception:
            n = prev
        if n == prev:
            stable += 1
            if stable >= 3:
                break
        else:
            stable = 0
        prev = n


async def _extract_reviews(page) -> list:
    """Parse all [data-review-id] blocks currently in the DOM into review dicts.

    Uses a single JS evaluate() call to read all data atomically — avoids
    stale-locator timeouts that occur when iterating per-element locators
    against a live DOM.
    """
    # First, expand all truncated review texts and reveal original (un-translated) text.
    try:
        await page.evaluate("""
        () => {
          for (const btn of document.querySelectorAll('button.w8nwRe')) {
            btn.click();
          }
          for (const el of document.querySelectorAll('button, span[role="button"]')) {
            if (el.textContent.trim() === 'See original') el.click();
          }
        }
        """)
        await page.wait_for_timeout(300)
    except Exception:
        pass

    raw = await page.evaluate("""
    () => {
      const results = [];
      const seen = new Set();
      // Support both old ([data-review-id]) and new (div.jftiEf) review card layouts.
      const cards = Array.from(document.querySelectorAll('[data-review-id], div.jftiEf'));
      for (const el of cards) {
        const rid = el.getAttribute('data-review-id') || '';
        // Skip cards that clearly have no reviewer identity.
        const hasName = el.querySelector('.d4r55, a[href*="/maps/contrib/"], [class*="fontHeadlineSmall"]');
        if (!hasName) continue;
        // Deduplicate: prefer data-review-id; fall back to name+date combo.
        const dedupeKey = rid || (hasName.innerText.trim().split('\\n')[0] + '|' + (el.querySelector('.rsqaWe, [class*="rsqaWe"]') || {innerText: ''}).innerText.trim());
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);

        // ── Reviewer name ────────────────────────────────────────────────────
        // Strategy 1 (most reliable): .d4r55 textContent — this element is the
        // dedicated name container and never includes badge text like "Local Guide".
        // textContent (not innerText) avoids headless-Chrome layout quirks where
        // inline siblings can appear on the same line as the name in innerText.
        let name = '';
        const nameEl0 = el.querySelector('a[href*="/maps/contrib/"] .d4r55')
                     || el.querySelector('.d4r55');
        if (nameEl0) {
          name = nameEl0.textContent.trim();
        }
        // Strategy 2: profile link innerText first-line — the photo link (which
        // shares the same href pattern) can appear first in the DOM, so we skip
        // it when its innerText is empty and fall through here only as a backup.
        if (!name) {
          const profileLink = el.querySelector('a[href*="/maps/contrib/"]');
          if (profileLink) {
            name = profileLink.innerText.trim().split('\\n')[0].trim();
          }
        }
        // Strategy 3: fontHeadlineSmall class (newer Maps layout fallback).
        if (!name) {
          const headlineEl = el.querySelector('[class*="fontHeadlineSmall"]');
          if (headlineEl) name = headlineEl.textContent.trim().split('\\n')[0].trim();
        }
        if (!name) continue;

        // ── Star rating ──────────────────────────────────────────────────────
        const starEl = el.querySelector('[aria-label*="star"]');
        const starLabel = starEl ? (starEl.getAttribute('aria-label') || '') : '';

        // ── Review text ──────────────────────────────────────────────────────
        // .MyEned  — review body text (stable across UI variants).
        // .wiI7pd  — newer replacement for .MyEned on some Map versions.
        // Owner responses live in a sibling wrapper (.CDe7pd / .bwb7ce); we
        // explicitly exclude them by restricting the query to elements that are
        // NOT descendants of any owner-response container.
        let text = '';
        const ownerRespEl = el.querySelector('[class*="CDe7pd"], [class*="bwb7ce"], [class*="RgaJbb"]');
        for (const sel of ['.MyEned', '.wiI7pd', '[class*="MyEned"]', '[class*="wiI7pd"]']) {
          const textEl = el.querySelector(sel);
          if (textEl && (!ownerRespEl || !ownerRespEl.contains(textEl))) {
            text = textEl.innerText.trim(); break;
          }
        }

        // ── Date ─────────────────────────────────────────────────────────────
        let date = '';
        for (const sel of ['.rsqaWe', '[class*="rsqaWe"]', '.DU9Pgb']) {
          const dateEl = el.querySelector(sel);
          if (dateEl) { date = dateEl.innerText.trim(); break; }
        }

        results.push({ rid: dedupeKey, name, starLabel, text, date });
      }
      return results;
    }
    """)

    seen_names_dates: set = set()
    reviews = []
    for item in raw:
        # JS already returns first-line-only names; strip any residual parentheticals.
        raw_name = item["name"].split("\n")[0].strip()
        name = re.sub(r"\s*\([^)]+\)\s*$", "", raw_name).strip()
        if not name:
            continue
        # Deduplicate at Python level too (belt-and-suspenders).
        key = (name, item["date"])
        if key in seen_names_dates:
            continue
        seen_names_dates.add(key)
        sm = re.search(r"(\d+)\s+star", item["starLabel"], re.I)
        stars = int(sm.group(1)) if sm else 0
        reviews.append({
            "reviewer_name": name,
            "rating": stars,
            "text": _clean_review_text(item["text"]),
            "date": item["date"],
        })
    return reviews


# ─────────────────────────────────────────────────────────────────────────────
# Review text cleanup — strip structured metadata injected below review body
# ─────────────────────────────────────────────────────────────────────────────

# Lines that are purely Google Maps structured metadata (sub-ratings, visit
# context, pricing) and must be removed from the user's review text.
#
# Each alternative must match the ENTIRE stripped line (anchored by ^...$).
# Patterns are listed from most-specific to least-specific to aid readability.
_META_LINE_RE = re.compile(
    r"""^(
        # ── Sub-rating lines ─────────────────────────────────────────────────
        # "Food: 1"  "Service: 5"  "Atmosphere: 3"  "Food: 5…"  "Atmosphere: …"
        (Food|Service|Atmosphere)\s*[:\s]\s*[\d.]*[.\u2026]*

        # ── Standalone visit/service attribute labels ─────────────────────────
        # These appear as bare label words, e.g. "Service", "Reservation"
        # Food/Service/Atmosphere also appear as section headers without a colon.
        | (Food|Service|Atmosphere|Meal\s+type|Dine\s+in|Takeout|Delivery
          |Price\s+per\s+person|Reservation|Wait\s+time|Parking|Accessibility)\s*[.\u2026]*

        # ── Meal period values (single-word or short) ─────────────────────────
        # "Dinner"  "Lunch"  "Breakfast"  "Brunch"  "Dine in…"  "Takeout…"
        | (Dinner|Lunch|Breakfast|Brunch|Dine\s+in|Takeout|Delivery)\s*[.\u2026]*

        # ── Attribute values (short phrases with no sentence punctuation) ─────
        # "Not sure"  "No wait"  "Free"  "Paid"
        | (Not\s+sure|No\s+wait|Short\s+wait|Long\s+wait|Free|Paid)\s*[.\u2026]*

        # ── Price / currency lines ─────────────────────────────────────────────
        # "kr 1–100"  "$ 50-100"  "€10–20"   (en dash, em dash, or hyphen)
        | [A-Za-z$€£¥]{1,4}\s*[\d][\d\s\u2013\u2014\-]*

        # ── Truncation artefacts ───────────────────────────────────────────────
        | [.\u2026]{2,}\s*More
        | More
        | [.\u2026]{2,}

        # ── Single-letter fragments (e.g. "T…") ──────────────────────────────
        | [A-Z][.\u2026]{1,3}
    )$""",
    re.VERBOSE | re.IGNORECASE,
)


def _clean_review_text(text: str) -> str:
    """
    Remove structured metadata lines that Google Maps renders below the review
    body (sub-ratings, visit type, price range, truncation artefacts).
    Only removes lines that consist *entirely* of metadata — genuine sentences
    that happen to contain those words are left intact.
    """
    if not text:
        return text
    cleaned = [line for line in text.splitlines() if not _META_LINE_RE.match(line.strip())]
    return "\n".join(cleaned).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Date-parsing helper (for recent-1-star path)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_weeks(date_str: str) -> int:
    """
    Convert a Google Maps relative date string to an age in whole weeks.

    Examples:
      "just now" / "X minutes ago" / "X hours ago" -> 0
      "X days ago"  -> 0  (< 1 week is reported as 0)
      "1 week ago"  -> 1
      "X weeks ago" -> X
      "1 month ago" -> 4
      "X months ago"-> X * 4
      "1 year ago"  -> 52
      "X years ago" -> X * 52
    """
    s = (date_str or "").lower().strip()
    if not s or "just now" in s:
        return 0
    if re.search(r"minute|hour", s):
        return 0
    m = re.search(r"(\d+)\s+day", s)
    if m:
        days = int(m.group(1))
        return 0 if days < 7 else days // 7
    if re.search(r"\ba\s+day\b|^1\s+day", s):
        return 0
    m = re.search(r"(\d+)\s+week", s)
    if m:
        return int(m.group(1))
    if re.search(r"\ba\s+week\b|^1\s+week", s):
        return 1
    m = re.search(r"(\d+)\s+month", s)
    if m:
        return int(m.group(1)) * 4
    if re.search(r"\ba\s+month\b|^1\s+month", s):
        return 4
    m = re.search(r"(\d+)\s+year", s)
    if m:
        return int(m.group(1)) * 52
    if re.search(r"\ba\s+year\b|^1\s+year", s):
        return 52
    return 0


async def _sort_reviews_newest_first(page) -> bool:
    """
    Click the Sort button then select 'Newest'.
    Returns True if the sort was applied successfully.
    Critical for the early-exit strategy in get_recent_one_star_reviews.
    """
    try:
        sort_btn = None
        for sel in [
            '[aria-label="Sort reviews"]',
            'button[aria-label*="Sort"]',
            'button[data-value="Sort"]',
            'button:has-text("Sort")',
        ]:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                sort_btn = loc
                break

        if sort_btn is None:
            return False

        await sort_btn.click(timeout=5_000)
        await page.wait_for_timeout(600)

        # Select "Newest" from the dropdown menu
        for sel in [
            '[data-index="1"]',
            '[role="menuitemradio"]:has-text("Newest")',
            '[role="option"]:has-text("Newest")',
            'li:has-text("Newest")',
            'div[id*="action-menu"] div:has-text("Newest")',
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible(timeout=1_500):
                    await el.click(timeout=3_000)
                    await page.wait_for_timeout(1_500)
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


async def _scroll_for_recent_reviews(page, max_age_weeks: int) -> list:
    """
    Scroll through reviews (sorted newest-first) and collect 1-star reviews
    within max_age_weeks.  Returns early as soon as a review exceeds the cutoff.

    Uses a single JS evaluate() per iteration to read all visible review data
    atomically — this avoids stale-locator timeouts that occur when the DOM
    changes (new reviews loaded) between .all() and per-element awaits.
    """
    seen_ids: set = set()
    one_star_reviews: list = []
    prev_count = 0
    stable = 0

    for _ in range(100):  # hard guard
        # Expand truncated texts and reveal original (un-translated) text.
        try:
            await page.evaluate("""
            () => {
              for (const btn of document.querySelectorAll('button.w8nwRe')) {
                btn.click();
              }
              for (const el of document.querySelectorAll('button, span[role="button"]')) {
                if (el.textContent.trim() === 'See original') el.click();
              }
            }
            """)
            await page.wait_for_timeout(300)
        except Exception:
            pass

        # Single JS snapshot — reads all review data synchronously against the
        # current DOM state.  No per-element Playwright locator calls needed.
        try:
            raw = await page.evaluate("""
            () => {
              const results = [];
              for (const el of document.querySelectorAll('[data-review-id], div.jftiEf')) {
                const hasName = el.querySelector('.d4r55, a[href*="/maps/contrib/"], [class*="fontHeadlineSmall"]');
                if (!hasName) continue;
                const rid = el.getAttribute('data-review-id') || '';

                // Reviewer name — .d4r55 textContent first (never includes badge),
                // profile link innerText as backup, hasName fallback last.
                let name = '';
                const nameEl0 = el.querySelector('a[href*="/maps/contrib/"] .d4r55')
                             || el.querySelector('.d4r55');
                if (nameEl0) {
                  name = nameEl0.textContent.trim();
                }
                if (!name) {
                  const profileLink = el.querySelector('a[href*="/maps/contrib/"]');
                  if (profileLink) name = profileLink.innerText.trim().split('\\n')[0].trim();
                }
                if (!name) name = hasName.innerText.trim().split('\\n')[0].trim();

                let date = '';
                for (const sel of ['.rsqaWe', '[class*="rsqaWe"]', '.DU9Pgb']) {
                  const dateEl = el.querySelector(sel);
                  if (dateEl) { date = dateEl.innerText.trim(); break; }
                }
                const starEl = el.querySelector('[aria-label*="star"]');
                const starLabel = starEl ? (starEl.getAttribute('aria-label') || '') : '';

                let text = '';
                const ownerRespEl = el.querySelector('[class*="CDe7pd"], [class*="bwb7ce"], [class*="RgaJbb"]');
                for (const sel of ['.MyEned', '.wiI7pd', '[class*="MyEned"]', '[class*="wiI7pd"]']) {
                  const textEl = el.querySelector(sel);
                  if (textEl && (!ownerRespEl || !ownerRespEl.contains(textEl))) {
                    text = textEl.innerText.trim(); break;
                  }
                }
                results.push({ rid, date, starLabel, name, text });
              }
              return results;
            }
            """)
        except Exception:
            break

        cutoff_hit = False
        for item in raw:
            rid = item["rid"]
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            date_str = item["date"]
            age_weeks = _parse_date_weeks(date_str)
            if age_weeks > max_age_weeks:
                cutoff_hit = True
                break  # sorted newest-first — all remaining are even older

            sm = re.search(r"(\d+)\s+star", item["starLabel"], re.I)
            stars = int(sm.group(1)) if sm else 0
            if stars != 1:
                continue

            name = re.sub(r"\s*\([^)]+\)\s*$", "", item["name"]).strip()
            one_star_reviews.append({
                "reviewer_name": name,
                "rating": stars,
                "text": _clean_review_text(item["text"]),
                "date": date_str,
                "date_weeks": age_weeks,
            })

        if cutoff_hit:
            break

        n = len(seen_ids)
        if n == prev_count:
            stable += 1
            if stable >= 3:
                break
        else:
            stable = 0
        prev_count = n

        try:
            await page.evaluate(_SCROLL_JS)
        except Exception:
            break
        await page.wait_for_timeout(600)

    return one_star_reviews


async def _extract_meta_from_page(page) -> "tuple[float, int]":
    """Extract (rating, review_count) from the rendered page. Returns (0.0, 0) on failure."""
    try:
        info = await page.evaluate(_META_JS)
        if info:
            return float(info["rating"]), int(info["count"])
    except Exception:
        pass

    try:
        title = await page.title()
        tm = re.search(r"([\d,.]+)\s*★", title)
        if tm:
            return float(tm.group(1).replace(",", ".")), 0
    except Exception:
        pass

    return 0.0, 0


async def _scrape_page(page, place_id: str, canonical_url: "str | None" = None) -> dict:
    """
    Core scraping logic that works on a single Playwright page object.

    If `canonical_url` is supplied (from the HTTP fast-path), the first browser
    navigation (overview page → canonical URL resolution) is skipped entirely,
    saving ~5-10 s per place.

    The page must belong to a context that already has consent cookies set.
    """
    out: dict = {"rating": 0.0, "review_count": 0, "reviews": []}

    # ── Step 1: resolve canonical URL (skipped if already known) ─────────────
    if not canonical_url or "/data=" not in canonical_url:
        await _safe_goto(
            page,
            f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en&gl=us",
        )
        await _handle_consent(page)
        try:
            await page.wait_for_url(re.compile(r"/maps/place/[^?]+/data="), timeout=12_000)
        except PWTimeout:
            pass
        canonical_url = page.url

        # Grab rating + count from overview page before we navigate away.
        try:
            await page.wait_for_selector(".F7nice, [aria-label*='star'], .jANrlb", timeout=6_000)
        except PWTimeout:
            pass
        rating, count = await _extract_meta_from_page(page)
        if rating:
            out["rating"] = rating
        if count:
            out["review_count"] = count

    # ── Step 2: navigate directly to Reviews tab via !9m1!1b1 flag ───────────
    reviews_url = _build_reviews_url(canonical_url)
    await _safe_goto(page, reviews_url)
    await _handle_consent(page)

    # Wait up to 12 s for the initial review blocks to appear (these are the
    # 3-review summary shown on the overview panel with !9m1!1b1 active).
    try:
        await page.wait_for_selector("[data-review-id]", timeout=12_000)
    except PWTimeout:
        pass

    # ── Step 3: open the full infinite-scroll reviews list ───────────────────
    # The !9m1!1b1 flag only activates the reviews *tab* — the full scrollable
    # list requires clicking "More reviews".  If the button is absent the place
    # has ≤3 reviews total and we already have everything we need.
    await _open_reviews_list(page)

    # ── Step 3b: extract rating + count NOW — the full panel is rendered ─────
    # .jANrlb ("4.8  18 reviews") is present on the full-list page.
    if not out["rating"] or not out["review_count"]:
        try:
            await page.wait_for_selector(".jANrlb, .F7nice", timeout=5_000)
        except PWTimeout:
            pass
        rating2, count2 = await _extract_meta_from_page(page)
        if rating2 and not out["rating"]:
            out["rating"] = rating2
        if count2 and not out["review_count"]:
            out["review_count"] = count2

    # ── Step 4: scroll to load ALL reviews ───────────────────────────────────
    await _scroll_all_reviews(page)

    # ── Step 5: extract review details ───────────────────────────────────────
    out["reviews"] = await _extract_reviews(page)

    # ── Step 6: last-chance rating + count if still missing ──────────────────
    if not out["rating"] or not out["review_count"]:
        rating3, count3 = await _extract_meta_from_page(page)
        if rating3 and not out["rating"]:
            out["rating"] = rating3
        if count3 and not out["review_count"]:
            out["review_count"] = count3

    return out


async def _pw_get_reviews_async(place_id: str, canonical_url: "str | None" = None) -> dict:
    """
    Full async Playwright scrape — creates its own browser, context, and page.
    Used by the sync get_reviews() fallback path.
    """
    async with async_playwright() as pw:
        bro = await pw.chromium.launch(headless=True)
        try:
            ctx = await bro.new_context(
                locale="en-US",
                user_agent=_UA,
                viewport={"width": 1280, "height": 900},
            )
            await ctx.add_cookies(_CONSENT_COOKIES_PW)
            page = await ctx.new_page()
            return await _scrape_page(page, place_id, canonical_url=canonical_url)
        finally:
            await bro.close()


# ─────────────────────────────────────────────────────────────────────────────
# Public API — single place (sync)
# ─────────────────────────────────────────────────────────────────────────────

def get_reviews(place_id: str) -> dict:
    """
    Fetch reviews for a Google Maps place.

    Returns:
        {
            "rating": float,          # overall star rating, e.g. 4.8
            "review_count": int,      # total number of reviews
            "reviews": [              # individual reviews (as many as available)
                {
                    "reviewer_name": str,
                    "rating": int,        # 1-5
                    "text": str,
                    "date": str           # relative string as-is, e.g. "8 months ago"
                }
            ]
        }
    """
    # Fast HTTP path: rating + review_count + canonical_url (no browser spin-up).
    meta = _http_get_meta(place_id)
    canonical_url = meta.get("canonical_url") if meta else None

    # Run async Playwright in a dedicated thread with its own event loop.
    # This is safe whether the caller is sync or already inside an asyncio loop.
    def _run() -> dict:
        return asyncio.run(_pw_get_reviews_async(place_id, canonical_url=canonical_url))

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(_run).result()

    # Prefer structured HTTP metadata when available.
    if meta:
        if meta.get("rating"):
            result["rating"] = meta["rating"]
        if meta.get("review_count"):
            result["review_count"] = meta["review_count"]

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Public API — async / batch
# ─────────────────────────────────────────────────────────────────────────────

async def get_reviews_async(place_id: str, context) -> dict:
    """
    Async version of get_reviews using a pre-created Playwright browser context.

    `context` is a Playwright BrowserContext that already has consent cookies set.
    Opens a new page, scrapes, closes the page, then returns the same dict as
    get_reviews().

    The HTTP meta fetch (canonical_url + rating/count) is fired concurrently
    with the initial page load so it adds zero wall-clock latency.
    """
    loop = asyncio.get_event_loop()

    # Fire HTTP meta fetch and page creation concurrently.
    meta_future = loop.run_in_executor(None, _http_get_meta, place_id)
    page = await context.new_page()

    try:
        # Await the HTTP result (likely already done by the time the page opens).
        meta = await meta_future
        canonical_url = meta.get("canonical_url") if meta else None

        result = await _scrape_page(page, place_id, canonical_url=canonical_url)
    finally:
        await page.close()

    if meta:
        if meta.get("rating"):
            result["rating"] = meta["rating"]
        if meta.get("review_count"):
            result["review_count"] = meta["review_count"]

    return result


async def scrape_batch(
    place_ids: list,
    workers: int = 10,
    progress_callback=None,
) -> dict:
    """
    Scrape multiple places concurrently.

    Args:
        place_ids:         list of Google Maps place IDs
        workers:           number of concurrent Playwright browser contexts (default 10)
        progress_callback: optional async callable(completed: int, total: int)

    Returns:
        dict mapping place_id -> result dict (same schema as get_reviews())
        Failed places get {"rating": 0.0, "review_count": 0, "reviews": [], "error": str}
    """
    results: dict = {}
    total = len(place_ids)
    completed = 0
    sem = asyncio.Semaphore(workers)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            # Pre-create one BrowserContext per worker slot — independent cookie
            # jars prevent cross-contamination between concurrent tabs.
            n_contexts = min(workers, total) if total > 0 else 1
            contexts = []
            for _ in range(n_contexts):
                ctx = await browser.new_context(
                    locale="en-US",
                    user_agent=_UA,
                    viewport={"width": 1280, "height": 900},
                )
                await ctx.add_cookies(_CONSENT_COOKIES_PW)
                contexts.append(ctx)

            # Queue acts as a counting semaphore over the context pool.
            ctx_queue: asyncio.Queue = asyncio.Queue()
            for ctx in contexts:
                await ctx_queue.put(ctx)

            async def _worker(place_id: str) -> None:
                nonlocal completed
                async with sem:
                    ctx = await ctx_queue.get()
                    try:
                        result = await get_reviews_async(place_id, ctx)
                        results[place_id] = result
                    except Exception as exc:
                        results[place_id] = {
                            "rating": 0.0,
                            "review_count": 0,
                            "reviews": [],
                            "error": str(exc),
                        }
                    finally:
                        await ctx_queue.put(ctx)
                        completed += 1
                        if progress_callback is not None:
                            await progress_callback(completed, total)

            await asyncio.gather(*[_worker(pid) for pid in place_ids])

        finally:
            for ctx in contexts:
                try:
                    await ctx.close()
                except Exception:
                    pass
            await browser.close()

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Public API — recent 1-star fast path (single place, async)
# ─────────────────────────────────────────────────────────────────────────────

async def get_recent_one_star_reviews(
    place_id: str,
    context,
    max_age_weeks: int = 4,
) -> dict:
    """
    Fast path: only fetch recent 1-star reviews.

    Sorts reviews by Newest first and exits as soon as a review exceeds
    max_age_weeks, so most well-rated businesses complete in ~15-20 s instead
    of the 50 s+ needed to scroll all reviews.

    Returns:
        {
            "rating": float,           # overall rating (from HTTP or Playwright)
            "review_count": int,       # total review count
            "one_star_reviews": [      # only 1-star reviews newer than max_age_weeks
                {
                    "reviewer_name": str,
                    "text": str,
                    "date": str,       # relative string e.g. "2 weeks ago"
                    "date_weeks": int, # parsed age in weeks
                }
            ]
        }
    """
    loop = asyncio.get_event_loop()

    # Fire HTTP meta fetch concurrently with page creation.
    meta_future = loop.run_in_executor(None, _http_get_meta, place_id)
    page = await context.new_page()

    out: dict = {"rating": 0.0, "review_count": 0, "one_star_reviews": []}

    try:
        meta = await meta_future
        canonical_url = meta.get("canonical_url") if meta else None

        if meta:
            if meta.get("rating"):
                out["rating"] = meta["rating"]
            if meta.get("review_count"):
                out["review_count"] = meta["review_count"]

        # ── Navigate to reviews ───────────────────────────────────────────────
        if not canonical_url or "/data=" not in canonical_url:
            await _safe_goto(
                page,
                f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en&gl=us",
            )
            await _handle_consent(page)
            try:
                await page.wait_for_url(
                    re.compile(r"/maps/place/[^?]+/data="), timeout=12_000
                )
            except PWTimeout:
                pass
            canonical_url = page.url

        reviews_url = _build_reviews_url(canonical_url)
        await _safe_goto(page, reviews_url)
        await _handle_consent(page)

        try:
            await page.wait_for_selector("[data-review-id]", timeout=12_000)
        except PWTimeout:
            pass

        # ── Open full scrollable list ─────────────────────────────────────────
        await _open_reviews_list(page)

        # ── Extract rating + count from page if HTTP didn't deliver them ──────
        if not out["rating"] or not out["review_count"]:
            try:
                await page.wait_for_selector(".jANrlb, .F7nice", timeout=5_000)
            except PWTimeout:
                pass
            rating, count = await _extract_meta_from_page(page)
            if rating and not out["rating"]:
                out["rating"] = rating
            if count and not out["review_count"]:
                out["review_count"] = count

        # ── Sort by Newest first — critical for early exit ────────────────────
        sorted_ok = await _sort_reviews_newest_first(page)
        if sorted_ok:
            # Wait for the list to re-render after sort.
            try:
                await page.wait_for_selector("[data-review-id]", timeout=8_000)
            except PWTimeout:
                pass

        # ── Scroll with early exit at age cutoff ──────────────────────────────
        out["one_star_reviews"] = await _scroll_for_recent_reviews(
            page, max_age_weeks
        )

        # ── Last-chance meta if still missing ────────────────────────────────
        if not out["rating"] or not out["review_count"]:
            rating2, count2 = await _extract_meta_from_page(page)
            if rating2 and not out["rating"]:
                out["rating"] = rating2
            if count2 and not out["review_count"]:
                out["review_count"] = count2

    finally:
        await page.close()

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Public API — batch monitor (async)
# ─────────────────────────────────────────────────────────────────────────────

async def monitor_batch(
    place_ids: list[str],
    workers: int = 20,
    max_age_weeks: int = 4,
    progress_callback=None,
) -> dict[str, dict]:
    """
    Batch version of get_recent_one_star_reviews.

    Uses the same browser/context pool pattern as scrape_batch.
    Returns dict mapping place_id -> result dict.
    Failed places get {"rating": 0.0, "review_count": 0, "one_star_reviews": [], "error": str}.
    """
    results: dict = {}
    total = len(place_ids)
    completed = 0
    sem = asyncio.Semaphore(workers)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            n_contexts = min(workers, total) if total > 0 else 1
            contexts = []
            for _ in range(n_contexts):
                ctx = await browser.new_context(
                    locale="en-US",
                    user_agent=_UA,
                    viewport={"width": 1280, "height": 900},
                )
                await ctx.add_cookies(_CONSENT_COOKIES_PW)
                contexts.append(ctx)

            ctx_queue: asyncio.Queue = asyncio.Queue()
            for ctx in contexts:
                await ctx_queue.put(ctx)

            async def _worker(place_id: str) -> None:
                nonlocal completed
                async with sem:
                    ctx = await ctx_queue.get()
                    try:
                        result = await get_recent_one_star_reviews(
                            place_id, ctx, max_age_weeks
                        )
                        results[place_id] = result
                    except Exception as exc:
                        results[place_id] = {
                            "rating": 0.0,
                            "review_count": 0,
                            "one_star_reviews": [],
                            "error": str(exc),
                        }
                    finally:
                        await ctx_queue.put(ctx)
                        completed += 1
                        if progress_callback is not None:
                            await progress_callback(completed, total)

            await asyncio.gather(*[_worker(pid) for pid in place_ids])

        finally:
            for ctx in contexts:
                try:
                    await ctx.close()
                except Exception:
                    pass
            await browser.close()

    return results
