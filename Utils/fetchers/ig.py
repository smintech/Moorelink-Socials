import asyncio
import json
import random
import time
import re
from typing import Dict, Optional, Any, Tuple, List, Set
from playwright.async_api import async_playwright, Page, BrowserContext, Error as PlaywrightError
import logging
from urllib.parse import quote
import os
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
#  Config guard — works with or without Utils
# ─────────────────────────────────────────────
try:
    from Utils import config
except ImportError:
    class config:
        POST_LIMIT = 10


# ══════════════════════════════════════════════
#  Network Strategy Constants
# ══════════════════════════════════════════════

# IMPORTANT: Instagram's SPA never resolves "load" or "domcontentloaded"
# reliably because of infinite background XHRs.  Always navigate with
# "commit" (first-byte received) and then manually wait for DOM content.
GOTO_WAIT_UNTIL = "commit"

# Single generous timeout — "commit" is instant so we won't hit this
# unless the server itself is unreachable
GOTO_TIMEOUT_MS = 30_000

# URL fragments that must NOT be blocked (CDN media we want)
CDN_ALLOWLIST = ("cdninstagram", "fbcdn")

# Instagram-specific "slow" paths that need more time for DOM settling
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

# Resource types to block — evaluated synchronously (no await needed)
_BLOCK_TYPES  = frozenset({"font", "stylesheet"})
_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net",
)


# ══════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════

class DetailedLogger:
    """
    Structured logger that produces readable, scannable output.

    Visual hierarchy:
      ╔══╗  Phase banners   — top-level milestones (browser start, profile load, …)
      ├──┤  Section headers — named sub-steps inside a phase
      │      Body lines     — info / success / warning / error
      └──    Tail lines     — completion of a section
      ····   Debug lines    — verbose detail, shown only at DEBUG level

    Timing: every phase banner and section header stamps an elapsed time
    from scraper start so it's easy to spot slow stages at a glance.
    """

    _ICONS = {
        "info":    "·",
        "success": "✓",
        "warning": "⚠",
        "error":   "✗",
        "debug":   "…",
    }

    def __init__(self, name: str = "IG Scraper"):
        self.name       = name
        self._start_ts  = time.monotonic()
        self._phase_ts  = self._start_ts
        self._phase_num = 0

        # One-time basicConfig — idempotent if called multiple times
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(message)s",          # we own the full line format
            datefmt="%H:%M:%S",
            force=True,
        )
        self._log = logging.getLogger(name)
        # Silence noisy Playwright / asyncio sub-loggers
        for noisy in ("playwright", "asyncio"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    # ── Internal helpers ────────────────────────────────────────────────

    def _elapsed(self) -> str:
        secs = time.monotonic() - self._start_ts
        return f"+{secs:5.1f}s"

    def _phase_elapsed(self) -> str:
        secs = time.monotonic() - self._phase_ts
        return f"{secs:.1f}s"

    def _ts(self) -> str:
        return time.strftime("%H:%M:%S")

    def _emit(self, level: int, line: str):
        self._log.log(level, line)

    # ── Public API ───────────────────────────────────────────────────────

    def phase(self, title: str, subtitle: str = ""):
        """
        Top-level phase banner. Use for major milestones.

        Example output:
          ╔══════════════════════════════════════════════════════╗
          ║  PHASE 2 · Load Profile                   +  2.1s  ║
          ║  https://www.instagram.com/nasa/                    ║
          ╚══════════════════════════════════════════════════════╝
        """
        self._phase_num += 1
        self._phase_ts   = time.monotonic()
        W = 60
        elapsed = self._elapsed()
        header  = f"  PHASE {self._phase_num} · {title}"
        padding = W - len(header) - len(elapsed) - 2
        top    = "╔" + "═" * W + "╗"
        mid    = f"║{header}{' ' * max(padding, 1)}{elapsed}  ║"
        self._emit(logging.INFO, "")
        self._emit(logging.INFO, top)
        self._emit(logging.INFO, mid)
        if subtitle:
            sub_line = f"║  {subtitle[:W-2]:<{W-2}}║"
            self._emit(logging.INFO, sub_line)
        self._emit(logging.INFO, "╚" + "═" * W + "╝")

    def section(self, title: str):
        """
        Named sub-step within a phase.

        Example output:
          ├─ [14:03:22] Navigation ────────────────────────────────
        """
        ts   = self._ts()
        line = f"  ├─ [{ts}] {title} "
        fill = max(0, 64 - len(line))
        self._emit(logging.INFO, line + "─" * fill)

    def section_end(self, summary: str = ""):
        """
        Close a section with an optional one-line summary.

        Example output:
          └─ done in 1.4s  ·  Found 12 post URLs
        """
        parts = [f"  └─ done in {self._phase_elapsed()}"]
        if summary:
            parts.append(f"  ·  {summary}")
        self._emit(logging.INFO, "".join(parts))

    def info(self, msg: str, indent: int = 1):
        pad = "     " * indent
        self._emit(logging.INFO,    f"{pad}{self._ICONS['info']}  {msg}")

    def success(self, msg: str, indent: int = 1):
        pad = "     " * indent
        self._emit(logging.INFO,    f"{pad}{self._ICONS['success']}  {msg}")

    def warning(self, msg: str, indent: int = 1):
        pad = "     " * indent
        self._emit(logging.WARNING, f"{pad}{self._ICONS['warning']}  {msg}")

    def error(self, msg: str, indent: int = 1):
        pad = "     " * indent
        self._emit(logging.ERROR,   f"{pad}{self._ICONS['error']}  {msg}")

    def debug(self, msg: str, indent: int = 1):
        pad = "     " * indent
        self._emit(logging.DEBUG,   f"{pad}{self._ICONS['debug']}  {msg}")

    def progress(self, done: int, total: int, label: str = ""):
        """
        Inline progress bar.

        Example:  ▓▓▓▓▓▓▓░░░  6/10  captions extracted
        """
        bar_w   = 10
        filled  = round(bar_w * done / max(total, 1))
        bar     = "▓" * filled + "░" * (bar_w - filled)
        suffix  = f"  {label}" if label else ""
        self._emit(logging.INFO, f"       [{bar}]  {done}/{total}{suffix}")

    def separator(self):
        self._emit(logging.INFO, "  " + "─" * 62)

    # Legacy shim — keeps old .step() calls working without crashing
    def step(self, title: str, details: str = ""):
        self.phase(title, details)


logger = DetailedLogger("Instagram Scraper")


# ══════════════════════════════════════════════
#  Result dataclass
# ══════════════════════════════════════════════

@dataclass
class ScrapingResult:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None


# ══════════════════════════════════════════════
#  Route / Resource Interceptor
# ══════════════════════════════════════════════

async def smart_route_handler(route):
    """
    Lean route handler — decision is pure Python (no extra awaits),
    so it never stalls the browser's network event queue.

    Key rules:
    • CDN media always allowed (we need those URLs later)
    • fonts / stylesheets blocked (useless for scraping)
    • known analytics domains blocked
    • everything else: continue immediately
    """
    url   = route.request.url
    rtype = route.request.resource_type

    # CDN images/video — must flow through so we can read their URLs
    if any(cdn in url for cdn in CDN_ALLOWLIST):
        await route.continue_()
        return

    # Cheap type check first (avoids string scan on most requests)
    if rtype in _BLOCK_TYPES:
        await route.abort()
        return

    # Analytics/tracking
    if any(d in url for d in _BLOCK_DOMAINS):
        await route.abort()
        return

    await route.continue_()


# ══════════════════════════════════════════════
#  Core Scraper
# ══════════════════════════════════════════════

class InstagramScraper:

    # ------------------------------------------------------------------
    def __init__(
        self,
        cookies: List[Dict],
        logger: DetailedLogger,
        max_concurrent: int = 3,
    ):
        self.cookies       = cookies
        self.logger        = logger
        self.max_concurrent = max_concurrent
        self.user_agents   = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
        self.ig_app_id   = "936619743392459"
        self.post_doc_id = "8845758582119845"
        self.csrf_token  = next(
            (c["value"] for c in cookies if c.get("name") == "csrftoken"), None
        )

    # ------------------------------------------------------------------
    #  Helpers
    # ------------------------------------------------------------------

    def _jitter(self, lo=0.4, hi=1.2) -> float:
        return random.uniform(lo, hi)

    async def _safe_evaluate(
        self,
        page: Page,
        script: str,
        timeout: float = 10.0,
        label: str = "evaluate",
    ) -> Any:
        """
        page.evaluate() with a hard Python-side asyncio timeout.

        WHY THIS EXISTS:
          Playwright's page.evaluate() has NO built-in timeout.  If the JS
          Promise never resolves (e.g. XHR dropped silently, page context
          destroyed mid-flight, Instagram rate-limit intercepting the request),
          the coroutine hangs indefinitely — freezing the worker.

          asyncio.wait_for() provides the outer kill-switch that Playwright
          doesn't.  On timeout we log and return None; the caller's fallback
          strategy then kicks in normally.
        """
        try:
            return await asyncio.wait_for(
                page.evaluate(script),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                f"[{label}] page.evaluate timed out after {timeout:.0f}s — skipping",
                indent=3,
            )
            return None
        except Exception as e:
            self.logger.debug(
                f"[{label}] evaluate error: {type(e).__name__}: {str(e).split(chr(10))[0][:80]}",
                indent=3,
            )
            return None

    # ------------------------------------------------------------------
    #  Navigation — the most resilient part
    # ------------------------------------------------------------------

    async def safe_goto(
        self,
        page: Page,
        url: str,
        max_retries: int = 3,
    ) -> bool:
        """
        Resilient navigation using "commit" wait strategy.

        WHY "commit":
          Instagram's SPA fires endless background XHRs, so "domcontentloaded"
          and "load" events may never fire — causing page.goto to hang until
          timeout even on a perfectly working connection.  "commit" resolves
          the moment the first byte of the response is received, which is
          always fast.  We then manually wait for DOM content ourselves.

        Retry strategy:
          Attempt 1 — commit, 30 s
          Attempt 2 — commit, 30 s  (after 2 s back-off)
          Attempt 3 — commit, 30 s  (after 4 s back-off, new page)
        """
        for attempt in range(max_retries):
            self.logger.debug(
                f"goto attempt {attempt + 1}/{max_retries}  "
                f"timeout={GOTO_TIMEOUT_MS // 1000}s  url={url[:80]}",
                indent=2,
            )
            try:
                response = await page.goto(
                    url,
                    wait_until=GOTO_WAIT_UNTIL,   # "commit" — never hangs
                    timeout=GOTO_TIMEOUT_MS,
                )

                if response is None:
                    self.logger.warning(f"attempt {attempt + 1} — goto returned no response, retrying", indent=2)
                    await asyncio.sleep(1.5)
                    continue

                status = response.status
                self.logger.debug(f"HTTP {status} received", indent=2)

                if status == 429:
                    wait = 5 + attempt * 3
                    self.logger.warning(f"Rate-limited (HTTP 429) — waiting {wait}s before retry", indent=2)
                    await asyncio.sleep(wait)
                    continue

                if status >= 500:
                    self.logger.warning(f"Server error (HTTP {status}) — retrying in {2 + attempt}s", indent=2)
                    await asyncio.sleep(2 + attempt)
                    continue

                if status < 400:
                    # Navigation committed — now wait for actual DOM content
                    # WITHOUT relying on load/domcontentloaded events
                    await self._wait_for_dom(page)
                    self.logger.debug(f"Page ready after attempt {attempt + 1}", indent=2)
                    return True

                # 4xx (not 429) — not retriable
                self.logger.error(f"Non-retriable HTTP {status} for {url[:60]}", indent=2)
                return False

            except PlaywrightError as e:
                err = str(e)
                # Trim the verbose Playwright call-log from the message
                short_err = err.split("\n")[0][:120]
                self.logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed — {short_err}", indent=2
                )

                is_timeout = "Timeout" in err or "timeout" in err
                is_net     = "net::ERR_" in err

                if (is_timeout or is_net) and attempt < max_retries - 1:
                    backoff = 2.0 * (attempt + 1) + random.uniform(0, 1)
                    self.logger.debug(f"Back-off {backoff:.1f}s before retry …", indent=2)
                    await asyncio.sleep(backoff)
                    continue

                # Non-retriable Playwright error — bubble up
                raise

        self.logger.error(
            f"Navigation failed — all {max_retries} attempts exhausted for {url[:70]}",
            indent=1,
        )
        return False

    async def _wait_for_dom(self, page: Page, timeout: float = 6.0):
        """
        Poll for DOM readiness after a "commit" navigation.

        Timeout is intentionally short (6s default) — we proceed even if
        nothing matches.  Hanging here starves workers.

        Selectors cover both post pages (article) and reel pages (video/section).
        """
        deadline = asyncio.get_event_loop().time() + timeout

        # Covers post pages, reel pages, and plain profile pages
        selectors = [
            "article",
            "video",              # reels land with a <video> before article hydrates
            "main",
            "section",
            "div[role='main']",
            "body > div > div",  # generic SPA shell
        ]

        for sel in selectors:
            remaining_ms = (deadline - asyncio.get_event_loop().time()) * 1000
            if remaining_ms <= 200:
                break
            try:
                await page.wait_for_selector(
                    sel,
                    state="attached",
                    timeout=min(remaining_ms, 2_000),   # max 2s per selector — was 5s
                )
                self.logger.debug(f"DOM ready — matched '{sel}'", indent=2)
                # Micro scroll to nudge lazy loaders
                await self._safe_evaluate(
                    page,
                    "window.scrollBy(0,80); window.scrollBy(0,-80)",
                    timeout=2.0,
                    label="scroll-jitter",
                )
                return
            except Exception:
                pass

        # Hard fallback — always proceed after budget expires
        elapsed = timeout - (deadline - asyncio.get_event_loop().time())
        self.logger.debug(
            f"DOM selectors did not match in {elapsed:.1f}s — proceeding anyway",
            indent=2,
        )
        await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    #  Popup dismissal
    # ------------------------------------------------------------------

    async def dismiss_popups(self, page: Page):
        dismissable = [
            'button:has-text("Not now")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept")',
            '[role="dialog"] button:has-text("Not Now")',
            'button:has-text("Allow essential and optional cookies")',
        ]
        for sel in dismissable:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=800):
                    await el.click(timeout=800)
                    await asyncio.sleep(0.15)
            except Exception:
                pass

    # ------------------------------------------------------------------
    #  Headers
    # ------------------------------------------------------------------

    def _build_headers(self) -> Dict[str, str]:
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.instagram.com",
            "referer": "https://www.instagram.com/",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="124", "Google Chrome";v="124"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agents[0],
            "x-csrftoken": self.csrf_token or "",
            "x-ig-app-id": self.ig_app_id,
            "x-ig-www-claim": "0",
            "x-requested-with": "XMLHttpRequest",
        }

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 1: GraphQL XHR (in-page)
    # ------------------------------------------------------------------

    async def extract_caption_graphql(self, page: Page, shortcode: str) -> Optional[str]:
        """
        Strategy 1: fire a GraphQL POST from inside the page via XHR.
        Hard capped at 9s Python-side (asyncio.wait_for) so it can never hang.
        XHR self-timeout is 7s so it always resolves before the outer cap.
        """
        headers  = self._build_headers()
        variables = {
            "shortcode": shortcode,
            "fetch_tagged_user_count": None,
            "hoisted_comment_id": None,
            "hoisted_reply_id": None,
        }
        body      = (
            f"variables={quote(json.dumps(variables, separators=(',', ':')))}"
            f"&doc_id={self.post_doc_id}"
        )
        safe_body = body.replace("'", "\\'")

        script = f"""
            () => new Promise((resolve) => {{
                const xhr = new XMLHttpRequest();
                xhr.open('POST', 'https://www.instagram.com/graphql/query', true);
                xhr.setRequestHeader('accept', '*/*');
                xhr.setRequestHeader('content-type', 'application/x-www-form-urlencoded');
                xhr.setRequestHeader('x-csrftoken', '{headers["x-csrftoken"]}');
                xhr.setRequestHeader('x-ig-app-id', '{headers["x-ig-app-id"]}');
                xhr.setRequestHeader('x-requested-with', 'XMLHttpRequest');
                xhr.setRequestHeader('referer', 'https://www.instagram.com/');
                xhr.timeout = 7000;
                xhr.onload    = () => {{
                    try {{ resolve(JSON.parse(xhr.responseText)); }}
                    catch (e) {{ resolve({{_err: 'parse:' + e.message}}); }}
                }};
                xhr.onerror   = () => resolve({{_err: 'network'}});
                xhr.ontimeout = () => resolve({{_err: 'xhr-timeout'}});
                xhr.send('{safe_body}');
            }})
        """

        result = await self._safe_evaluate(page, script, timeout=9.0, label="graphql-xhr")
        if not result:
            return None
        if "_err" in result:
            self.logger.debug(f"GraphQL XHR: {result['_err']}", indent=3)
            return None

        media = result.get("data", {}).get("xdt_shortcode_media", {})
        if not media:
            return None
        edges = media.get("edge_media_to_caption", {}).get("edges", [])
        if edges:
            return edges[0]["node"].get("text") or None
        return media.get("accessibility_caption") or None

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 2: DOM selectors
    # ------------------------------------------------------------------

    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        """
        Strategy 2: synchronous DOM scan — runs in <1ms, capped at 4s just in case.
        """
        script = r"""
            () => {
                const selectors = [
                    'div._aacl._a9zr._a9zo._a9z9',
                    'div._aacl._a9zr',
                    'div[data-testid="post-caption"]',
                    'article div[role="button"] + div span',
                    'h1 + div span',
                    'span._aacl',
                ];
                for (const s of selectors) {
                    const el = document.querySelector(s);
                    if (el) {
                        const t = el.innerText?.trim();
                        if (t && t.length > 5) return { text: t, src: s };
                    }
                }
                const meta = document.querySelector('meta[property="og:description"]');
                if (meta) {
                    const c = meta.getAttribute('content') || '';
                    const cleaned = c.replace(/^[^,]+,\s*/, '').trim();
                    if (cleaned.length > 20) return { text: cleaned, src: 'og:description' };
                }
                return null;
            }
        """
        res = await self._safe_evaluate(page, script, timeout=4.0, label="dom-caption")
        if res and res.get("text"):
            return re.sub(r"\s*more\s*$", "", res["text"], flags=re.IGNORECASE).strip()
        return None

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 3: JSON-LD
    # ------------------------------------------------------------------

    async def extract_caption_from_ldjson(self, page: Page) -> Optional[str]:
        """Strategy 3: synchronous JSON-LD scan, capped at 4s."""
        script = r"""
            () => {
                for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d.caption || d.description) return d.caption || d.description;
                    } catch (_) {}
                }
                return null;
            }
        """
        return await self._safe_evaluate(page, script, timeout=4.0, label="ldjson-caption")

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 4: window globals
    # ------------------------------------------------------------------

    async def extract_caption_from_window_data(self, page: Page) -> Optional[str]:
        """Strategy 4: read Instagram's pre-loaded window globals, capped at 4s."""
        script = r"""
            () => {
                try {
                    for (const k of Object.keys(window.__additionalData || {})) {
                        const m = window.__additionalData[k]?.data?.graphql?.shortcode_media
                               || window.__additionalData[k]?.data?.xdt_shortcode_media;
                        if (m) {
                            const edges = m.edge_media_to_caption?.edges || [];
                            if (edges.length) return edges[0].node.text;
                        }
                    }
                } catch (_) {}
                try {
                    const m = window._sharedData?.entry_data?.PostPage?.[0]?.graphql?.shortcode_media;
                    if (m) {
                        const edges = m.edge_media_to_caption?.edges || [];
                        if (edges.length) return edges[0].node.text;
                    }
                } catch (_) {}
                return null;
            }
        """
        return await self._safe_evaluate(page, script, timeout=4.0, label="window-globals")

    # ------------------------------------------------------------------
    #  Master caption orchestrator
    # ------------------------------------------------------------------

    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
        """
        Master caption orchestrator — tries four strategies in priority order.

        Hard outer cap: 30s total.  Even if every strategy hangs at its own
        sub-timeout, this guarantees the worker is never blocked longer than
        this.  In practice the inner _safe_evaluate timeouts (4–9s each) mean
        we exit well before the outer cap.
        """
        async def _inner() -> str:
            strategies = [
                ("GraphQL XHR",   lambda: self.extract_caption_graphql(page, shortcode) if shortcode else asyncio.coroutine(lambda: None)()),
                ("Window data",   lambda: self.extract_caption_from_window_data(page)),
                ("DOM selectors", lambda: self.extract_caption_from_dom(page)),
                ("JSON-LD",       lambda: self.extract_caption_from_ldjson(page)),
            ]
            for name, fn in strategies:
                try:
                    result = await fn()
                    if result:
                        self.logger.debug(
                            f"Caption via [{name}]  {len(result)} chars", indent=2
                        )
                        return result
                    else:
                        self.logger.debug(f"[{name}] returned empty — trying next", indent=2)
                except Exception as e:
                    self.logger.debug(
                        f"[{name}] raised {type(e).__name__}: {str(e)[:60]}", indent=2
                    )
            self.logger.warning("All caption strategies exhausted — no caption found", indent=2)
            return ""

        try:
            return await asyncio.wait_for(_inner(), timeout=30.0)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Caption extraction hit the 30s hard cap — returning empty", indent=2
            )
            return ""

    # ------------------------------------------------------------------
    #  Media URL extraction
    # ------------------------------------------------------------------

    async def extract_media_from_post(
        self, page: Page, post_url: str
    ) -> Tuple[str, bool]:
        is_video = any(p in post_url for p in ("/reel/", "/tv/"))

        if is_video:
            script = r"""
                () => {
                    const v = document.querySelector('video[src]');
                    if (v?.src && !v.src.startsWith('blob')) return v.src;
                    const vs = document.querySelector('video source[src]');
                    if (vs?.src && !vs.src.startsWith('blob')) return vs.src;
                    const mv = document.querySelector('meta[property="og:video"]');
                    if (mv) return mv.getAttribute('content');
                    try {
                        const keys = Object.keys(window.__additionalData || {});
                        for (const k of keys) {
                            const m = window.__additionalData[k]?.data?.xdt_shortcode_media;
                            if (m?.video_url) return m.video_url;
                        }
                    } catch (_) {}
                    return '';
                }
            """
        else:
            script = r"""
                () => {
                    const mi = document.querySelector('meta[property="og:image"]');
                    if (mi) {
                        const c = mi.getAttribute('content');
                        if (c && (c.includes('cdninstagram') || c.includes('fbcdn'))) return c;
                    }
                    const imgs = Array.from(document.querySelectorAll('article img[src]'));
                    const cands = imgs
                        .filter(i => (i.src.includes('cdninstagram') || i.src.includes('fbcdn'))
                                  && !i.src.includes('profile') && i.naturalWidth > 300)
                        .sort((a, b) => b.naturalWidth - a.naturalWidth);
                    return cands[0]?.src || '';
                }
            """

        url = await self._safe_evaluate(page, script, timeout=6.0, label="media-url")
        if url and not url.startswith("blob"):
            return url, is_video

        return "", is_video

    # ------------------------------------------------------------------
    #  Single post scraper (isolated page)
    # ------------------------------------------------------------------

    async def scrape_single_post(
        self,
        context: BrowserContext,
        post_url: str,
        shortcode: str,
        post_index: int,
    ) -> ScrapingResult:
        page = None
        t0 = time.monotonic()
        post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"

        self.logger.info(
            f"[{post_index:>2}] {post_type}  {shortcode}",
            indent=1,
        )

        try:
            page = await context.new_page()
            page.set_default_navigation_timeout(40_000)
            page.set_default_timeout(15_000)
            await page.route("**/*", smart_route_handler)

            # ── Navigate ──────────────────────────────────────────────
            self.logger.debug(f"[{post_index}] navigating …", indent=2)
            if not await self.safe_goto(page, post_url, max_retries=3):
                return ScrapingResult(success=False, error="Navigation failed after all retries")

            if "accounts/login" in page.url:
                self.logger.error(f"[{post_index}] redirected to login — session expired", indent=2)
                return ScrapingResult(success=False, error="Redirected to login")

            await self.dismiss_popups(page)
            await self._wait_for_dom(page)

            # ── Extract ───────────────────────────────────────────────
            self.logger.debug(f"[{post_index}] extracting caption …", indent=2)
            caption = await self.extract_caption_from_post(page, shortcode)

            self.logger.debug(f"[{post_index}] extracting media URL …", indent=2)
            media_url, is_video = await self.extract_media_from_post(page, post_url)

            elapsed = time.monotonic() - t0

            if media_url and not media_url.startswith("blob"):
                kind = "VIDEO" if is_video else "IMAGE"
                cap_info = f"{len(caption)} chars" if caption else "no caption"
                self.logger.success(
                    f"[{post_index:>2}] ✓ {kind:<5}  {shortcode:<20}  "
                    f"caption: {cap_info:<18}  {elapsed:.1f}s",
                    indent=1,
                )
                return ScrapingResult(
                    success=True,
                    data={
                        "url":       post_url,
                        "shortcode": shortcode,
                        "caption":   caption,
                        "media_url": media_url,
                        "is_video":  is_video,
                    },
                )

            self.logger.warning(
                f"[{post_index:>2}] no media URL found  shortcode={shortcode}  {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(success=False, error="No media URL found")

        except Exception as e:
            elapsed = time.monotonic() - t0
            self.logger.error(
                f"[{post_index:>2}] {type(e).__name__}: {str(e).split(chr(10))[0][:80]}  "
                f"{elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(success=False, error=str(e)[:80])
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    #  Profile scraper (scroll + collect post URLs)
    # ------------------------------------------------------------------

    async def _collect_post_urls(
        self, page: Page, post_limit: int
    ) -> List[str]:
        """
        Scroll the profile grid, collecting post URLs.
        Handles Instagram's lazy-loaded infinite scroll robustly.
        """
        post_urls: List[str] = []
        last_height = 0
        stale_rounds = 0
        MAX_STALE = 3
        MAX_SCROLLS = 15

        js_collect = r"""
            () => {
                const anchors = Array.from(
                    document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]')
                );
                return [...new Set(anchors.map(a => a.href.split('?')[0]))];
            }
        """

        for i in range(MAX_SCROLLS):
            links = await self._safe_evaluate(page, js_collect, timeout=5.0, label="collect-links") or []
            new = [u for u in links if u not in post_urls]
            post_urls.extend(new)

            newly = len(new)
            total = len(post_urls)
            if newly:
                self.logger.info(
                    f"Scroll {i+1:>2}/{MAX_SCROLLS}  "
                    f"+{newly} new  →  {total} total",
                    indent=2,
                )
            else:
                self.logger.debug(
                    f"Scroll {i+1:>2}/{MAX_SCROLLS}  no new posts  "
                    f"(stale {stale_rounds + 1}/{MAX_STALE})",
                    indent=2,
                )

            if total >= post_limit:
                self.logger.info(f"Post limit ({post_limit}) reached — stopping scroll", indent=2)
                break

            # Scroll to bottom
            await self._safe_evaluate(
                page, "window.scrollTo(0, document.body.scrollHeight)", timeout=3.0, label="scroll"
            )

            # Wait for new content — poll for height change up to ~2s
            for _ in range(4):
                await asyncio.sleep(0.5)
                new_height = await self._safe_evaluate(
                    page, "document.body.scrollHeight", timeout=3.0, label="scroll-height"
                ) or last_height
                if new_height != last_height:
                    last_height = new_height
                    stale_rounds = 0
                    break
            else:
                stale_rounds += 1

            if stale_rounds >= MAX_STALE:
                self.logger.info(
                    f"Page end reached — no new content after {MAX_STALE} consecutive scrolls",
                    indent=2,
                )
                break

        return post_urls[:post_limit]

    # ------------------------------------------------------------------
    #  Main entry point
    # ------------------------------------------------------------------

    async def scrape_profile(
        self, username: str, post_limit: int = 10
    ) -> List[Dict]:
        t_total = time.monotonic()

        self.logger.phase(
            "Instagram Scraper",
            f"@{username}  ·  limit {post_limit} posts  ·  {self.max_concurrent} workers",
        )

        async with async_playwright() as p:

            # ── Browser startup ───────────────────────────────────────
            self.logger.section("Browser")
            self.logger.info("Launching Chromium (headless) …", indent=2)
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                    "--disable-setuid-sandbox",
                    "--ignore-certificate-errors",
                    "--dns-prefetch-disable",
                ],
            )
            self.logger.success("Browser ready", indent=2)
            self.logger.section_end()

            # ── Profile context ───────────────────────────────────────
            main_ctx = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
                java_script_enabled=True,
                ignore_https_errors=True,
            )
            await main_ctx.add_cookies(self.cookies)
            self.logger.debug(f"Session loaded with {len(self.cookies)} cookies", indent=2)

            try:
                # ── Phase: Load profile ───────────────────────────────
                self.logger.phase("Load Profile", f"https://www.instagram.com/{username}/")
                self.logger.section("Navigation")
                profile_page = await main_ctx.new_page()
                await profile_page.route("**/*", smart_route_handler)

                profile_url = f"https://www.instagram.com/{username}/"
                if not await self.safe_goto(profile_page, profile_url, max_retries=3):
                    self.logger.error("Could not load profile — aborting", indent=1)
                    return []

                if "accounts/login" in profile_page.url:
                    self.logger.error(
                        "Redirected to login page — cookies are expired or invalid", indent=1
                    )
                    await profile_page.close()
                    return []

                self.logger.success(f"Profile page loaded: {profile_page.url[:70]}", indent=2)
                self.logger.section_end()

                await self.dismiss_popups(profile_page)
                await self._wait_for_dom(profile_page)

                # ── Phase: Discover posts ─────────────────────────────
                self.logger.phase("Discover Posts", f"Scrolling grid — target: {post_limit} posts")
                self.logger.section("Grid scroll")
                post_urls = await self._collect_post_urls(profile_page, post_limit)
                await profile_page.close()
                self.logger.section_end(f"{len(post_urls)} unique post URLs collected")

                if not post_urls:
                    self.logger.error("No post URLs found — profile may be private or empty", indent=1)
                    return []

                self.logger.separator()
                for i, u in enumerate(post_urls, 1):
                    sc = u.split("/p/")[-1].split("/reel/")[-1].split("/tv/")[-1].split("/")[0]
                    self.logger.debug(f"  {i:>2}.  {sc:<20}  {u}", indent=1)
                self.logger.separator()

                # ── Phase: Scrape posts ───────────────────────────────
                self.logger.phase(
                    "Scrape Posts",
                    f"{len(post_urls)} posts  ·  {self.max_concurrent} concurrent workers",
                )

                # One isolated context per worker
                self.logger.section("Worker contexts")
                worker_contexts: List[BrowserContext] = []
                for w in range(self.max_concurrent):
                    wctx = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        viewport={"width": 1920, "height": 1080},
                        locale="en-US",
                        ignore_https_errors=True,
                    )
                    await wctx.add_cookies(self.cookies)
                    worker_contexts.append(wctx)
                self.logger.success(
                    f"{self.max_concurrent} worker contexts ready", indent=2
                )
                self.logger.section_end()

                semaphore = asyncio.Semaphore(self.max_concurrent)

                def _shortcode(url: str, idx: int) -> str:
                    for seg in ("/p/", "/reel/", "/tv/"):
                        if seg in url:
                            return url.split(seg)[-1].split("/")[0]
                    return f"post_{idx}"

                async def _worker(idx: int, url: str) -> ScrapingResult:
                    async with semaphore:
                        ctx = worker_contexts[idx % self.max_concurrent]
                        sc  = _shortcode(url, idx)
                        return await self.scrape_single_post(ctx, url, sc, idx + 1)

                self.logger.section("Post scraping")
                tasks   = [_worker(i, u) for i, u in enumerate(post_urls)]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                self.logger.section_end()

                # ── Tally results ─────────────────────────────────────
                posts:    List[Dict] = []
                failures: List[str]  = []

                for i, res in enumerate(results):
                    if isinstance(res, ScrapingResult) and res.success:
                        posts.append(res.data)
                    elif isinstance(res, ScrapingResult):
                        failures.append(f"  post {i+1}: {res.error}")
                    elif isinstance(res, Exception):
                        failures.append(f"  post {i+1}: {type(res).__name__}: {str(res)[:60]}")

                for wctx in worker_contexts:
                    try:
                        await wctx.close()
                    except Exception:
                        pass

                # ── Final summary ─────────────────────────────────────
                elapsed_total = time.monotonic() - t_total
                self.logger.phase(
                    "Summary",
                    f"Total time: {elapsed_total:.1f}s",
                )
                self.logger.separator()
                self.logger.success(
                    f"Scraped:  {len(posts)}/{len(post_urls)} posts", indent=1
                )
                if failures:
                    self.logger.warning(
                        f"Failed:   {len(failures)}/{len(post_urls)} posts", indent=1
                    )
                    for f in failures:
                        self.logger.debug(f, indent=2)

                videos = sum(1 for p in posts if p.get("is_video"))
                images = len(posts) - videos
                captioned = sum(1 for p in posts if p.get("caption"))
                self.logger.info(f"Images:   {images}", indent=1)
                self.logger.info(f"Videos:   {videos}", indent=1)
                self.logger.info(f"With caption: {captioned}/{len(posts)}", indent=1)
                self.logger.separator()

                self.logger.progress(len(posts), len(post_urls), "posts scraped")

                return posts

            except Exception as e:
                import traceback
                self.logger.error(f"Fatal error: {type(e).__name__}: {str(e)[:80]}", indent=1)
                self.logger.debug(traceback.format_exc(), indent=1)
                return []
            finally:
                self.logger.section("Cleanup")
                try:
                    await main_ctx.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                    self.logger.success("Browser closed", indent=2)
                except Exception:
                    pass
                self.logger.section_end()


# ══════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════

async def fetch_ig_urls(
    account: str,
    cookies: List[Dict[str, Any]] = None,
    max_concurrent: int = 3,
) -> List[Dict[str, Any]]:
    """
    Scrape an Instagram profile and return post data.

    Returns:
        List of dicts with keys: url, shortcode, caption, media_url, is_video
    """
    account = account.lstrip("@")

    logger.phase("fetch_ig_urls", f"account=@{account}  workers={max_concurrent}")
    logger.section("Cookie setup")

    if cookies is None:
        logger.info("No cookies passed — reading IG_COOKIES env var", indent=2)
        raw = os.getenv("IG_COOKIES", "")
        if not raw:
            logger.error("IG_COOKIES is not set — cannot authenticate", indent=2)
            return []
        try:
            cookies = json.loads(raw)
            logger.success(f"Loaded {len(cookies)} cookies from environment", indent=2)
        except json.JSONDecodeError as e:
            logger.error(f"IG_COOKIES is not valid JSON: {e}", indent=2)
            return []
    else:
        logger.success(f"Using {len(cookies)} caller-supplied cookies", indent=2)

    csrf_ok = any(c.get("name") == "csrftoken" for c in cookies)
    session_ok = any(c.get("name") == "sessionid" for c in cookies)
    logger.info(f"csrftoken present: {csrf_ok}  |  sessionid present: {session_ok}", indent=2)
    if not csrf_ok:
        logger.warning("csrftoken missing — GraphQL caption strategy will likely fail", indent=2)
    if not session_ok:
        logger.warning("sessionid missing — profile may not load as authenticated", indent=2)

    logger.section_end()

    scraper = InstagramScraper(cookies=cookies, logger=logger, max_concurrent=max_concurrent)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )