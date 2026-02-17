import asyncio
import json
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
import os
from dataclasses import dataclass

from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    Error as PlaywrightError,
)
import logging

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

GOTO_WAIT_UNTIL = "commit"
GOTO_TIMEOUT_MS = 30_000

CDN_ALLOWLIST      = ("cdninstagram", "fbcdn")
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

_BLOCK_TYPES   = frozenset({"font", "stylesheet"})
_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net",
)


# ══════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════

class DetailedLogger:
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

        logging.basicConfig(level=logging.DEBUG, format="%(message)s", force=True)
        self._log = logging.getLogger(name)
        for noisy in ("playwright", "asyncio"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    def _elapsed(self) -> str:
        return f"+{time.monotonic() - self._start_ts:5.1f}s"

    def _phase_elapsed(self) -> str:
        return f"{time.monotonic() - self._phase_ts:.1f}s"

    def _ts(self) -> str:
        return time.strftime("%H:%M:%S")

    def _emit(self, level: int, line: str):
        self._log.log(level, line)

    def phase(self, title: str, subtitle: str = ""):
        self._phase_num += 1
        self._phase_ts   = time.monotonic()
        W       = 60
        elapsed = self._elapsed()
        header  = f"  PHASE {self._phase_num} · {title}"
        padding = W - len(header) - len(elapsed) - 2
        self._emit(logging.INFO, "")
        self._emit(logging.INFO, "╔" + "═" * W + "╗")
        self._emit(logging.INFO, f"║{header}{' ' * max(padding, 1)}{elapsed}  ║")
        if subtitle:
            self._emit(logging.INFO, f"║  {subtitle[:W-2]:<{W-2}}║")
        self._emit(logging.INFO, "╚" + "═" * W + "╝")

    def section(self, title: str):
        ts   = self._ts()
        line = f"  ├─ [{ts}] {title} "
        self._emit(logging.INFO, line + "─" * max(0, 64 - len(line)))

    def section_end(self, summary: str = ""):
        parts = [f"  └─ done in {self._phase_elapsed()}"]
        if summary:
            parts.append(f"  ·  {summary}")
        self._emit(logging.INFO, "".join(parts))

    def info(self, msg: str, indent: int = 1):
        self._emit(logging.INFO,    f"{'     ' * indent}{self._ICONS['info']}  {msg}")

    def success(self, msg: str, indent: int = 1):
        self._emit(logging.INFO,    f"{'     ' * indent}{self._ICONS['success']}  {msg}")

    def warning(self, msg: str, indent: int = 1):
        self._emit(logging.WARNING, f"{'     ' * indent}{self._ICONS['warning']}  {msg}")

    def error(self, msg: str, indent: int = 1):
        self._emit(logging.ERROR,   f"{'     ' * indent}{self._ICONS['error']}  {msg}")

    def debug(self, msg: str, indent: int = 1):
        self._emit(logging.DEBUG,   f"{'     ' * indent}{self._ICONS['debug']}  {msg}")

    def progress(self, done: int, total: int, label: str = ""):
        bar_w  = 10
        filled = round(bar_w * done / max(total, 1))
        bar    = "▓" * filled + "░" * (bar_w - filled)
        suffix = f"  {label}" if label else ""
        self._emit(logging.INFO, f"       [{bar}]  {done}/{total}{suffix}")

    def separator(self):
        self._emit(logging.INFO, "  " + "─" * 62)

    # Legacy shim
    def step(self, title: str, details: str = ""):
        self.phase(title, details)


logger = DetailedLogger("Instagram Scraper")


# ══════════════════════════════════════════════
#  Result dataclass
# ══════════════════════════════════════════════

@dataclass
class ScrapingResult:
    success: bool
    data:    Optional[Dict] = None
    error:   Optional[str]  = None


# ══════════════════════════════════════════════
#  Route / Resource Interceptor
# ══════════════════════════════════════════════

async def smart_route_handler(route):
    """
    Block fonts, stylesheets, and analytics.  Allow CDN media.
    Wrapped in try/except so closing a page mid-request never floods
    the logs with unhandled TargetClosedError futures.
    """
    try:
        url   = route.request.url
        rtype = route.request.resource_type

        if any(cdn in url for cdn in CDN_ALLOWLIST):
            await route.continue_()
            return
        if rtype in _BLOCK_TYPES:
            await route.abort()
            return
        if any(d in url for d in _BLOCK_DOMAINS):
            await route.abort()
            return
        await route.continue_()
    except PlaywrightError:
        pass


# ══════════════════════════════════════════════
#  HTML-level data extractor
# ══════════════════════════════════════════════

class InstagramHtmlParser:
    """
    Pure-Python parser for Instagram's server-rendered HTML.

    WHY THIS EXISTS
    ───────────────
    Instagram often embeds all post/reel data directly in the initial HTML
    (inside <script> tags as minified JSON blobs) rather than issuing
    separate API calls after page load.  This means:

      • Response intercept misses it  — no XHR to catch
      • page.evaluate() can't read it — JS thread saturated on
                                        CPU-constrained servers

    Reading the raw response *body bytes* at the network layer and parsing
    with Python regex+json bypasses both problems entirely.

    Patterns covered
    ────────────────
    1. JSON-LD  <script type="application/ld+json">
    2. og:video / og:image / og:description meta tags
    3. shortcode_media / xdt_shortcode_media JSON blobs (inline script data)
    4. Raw video_url / display_url key scan across entire HTML
    5. edge_media_to_caption pattern
    """

    _RE_VIDEO_URL    = re.compile(r'"video_url"\s*:\s*"(https://[^"]+\.mp4[^"]*)"')
    _RE_DISPLAY_URL  = re.compile(r'"display_url"\s*:\s*"(https://[^"]+(?:cdninstagram|fbcdn)[^"]*)"')
    _RE_OG_VIDEO     = re.compile(r'<meta[^>]+property=["\']og:video["\'][^>]+content=["\']([^"\']+)["\']', re.I)
    _RE_OG_IMAGE     = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I)
    _RE_OG_DESC      = re.compile(
        r'<meta[^>]+(?:property=["\']og:description["\']|name=["\']description["\'])'
        r'[^>]+content=["\']([^"\']{20,})["\']', re.I
    )
    _RE_LDJSON       = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.DOTALL | re.I,
    )
    _RE_SHORTCODE_MEDIA = re.compile(
        r'"(?:shortcode_media|xdt_shortcode_media)"\s*:\s*(\{)',
    )
    _RE_EDGE_CAPTION = re.compile(
        r'"edge_media_to_caption"\s*:\s*\{[^}]*"edges"\s*:\s*\[\s*\{[^}]*"node"\s*:\s*'
        r'\{[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        re.DOTALL,
    )

    @classmethod
    def _unescape(cls, s: str) -> str:
        try:
            return json.loads(f'"{s}"')
        except Exception:
            return s.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")

    @classmethod
    def _find_json_object(cls, text: str, start: int) -> Optional[str]:
        """
        Extract the complete JSON object whose opening '{' is at `start`.
        Uses a brace-counter with string-escape awareness.
        """
        depth  = 0
        in_str = False
        escape = False
        i      = start
        n      = len(text)
        while i < n:
            c = text[i]
            if escape:
                escape = False
            elif c == "\\" and in_str:
                escape = True
            elif c == '"':
                in_str = not in_str
            elif not in_str:
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        return text[start : i + 1]
            i += 1
        return None

    @classmethod
    def parse(
        cls, html: bytes, shortcode: str, is_video: bool
    ) -> Tuple[Optional[str], Optional[str], bool]:
        """
        Parse raw HTML bytes.  Returns (caption, media_url, is_video).
        Any value may be None if not found.
        """
        try:
            text = html.decode("utf-8", errors="ignore")
        except Exception:
            return None, None, is_video

        caption:   Optional[str] = None
        media_url: Optional[str] = None

        # ── 1. JSON-LD ────────────────────────────────────────────────
        for m in cls._RE_LDJSON.finditer(text):
            try:
                blob = json.loads(m.group(1))
                if isinstance(blob, list):
                    blob = blob[0] if blob else {}
                cap = blob.get("caption") or blob.get("description") or ""
                vid = blob.get("contentUrl") or blob.get("embedUrl") or ""
                img = blob.get("thumbnailUrl") or blob.get("image") or ""
                if isinstance(img, list):
                    img = img[0] if img else ""
                if cap and not caption:
                    caption = cap
                if vid and not media_url:
                    media_url = vid
                    is_video  = True
                elif img and not media_url:
                    media_url = img
                if caption and media_url:
                    return caption, media_url, is_video
            except Exception:
                pass

        # ── 2. og: meta tags ─────────────────────────────────────────
        if not media_url:
            m = cls._RE_OG_VIDEO.search(text)
            if m:
                media_url = m.group(1)
                is_video  = True
        if not media_url:
            m = cls._RE_OG_IMAGE.search(text)
            if m and any(cdn in m.group(1) for cdn in CDN_ALLOWLIST):
                media_url = m.group(1)
        if not caption:
            m = cls._RE_OG_DESC.search(text)
            if m:
                raw     = cls._unescape(m.group(1))
                cleaned = re.sub(r"^[^:]+:\s*", "", raw).strip()
                if len(cleaned) > 10:
                    caption = cleaned

        # ── 3. shortcode_media JSON blob ──────────────────────────────
        for m in cls._RE_SHORTCODE_MEDIA.finditer(text):
            blob_str = cls._find_json_object(text, m.start(1))
            if not blob_str:
                continue
            try:
                media = json.loads(blob_str)
            except Exception:
                continue

            if not caption:
                edges = media.get("edge_media_to_caption", {}).get("edges", [])
                if edges:
                    caption = edges[0].get("node", {}).get("text")

            if not media_url:
                if media.get("video_url"):
                    is_video  = True
                    media_url = media["video_url"]
                elif media.get("display_url"):
                    media_url = media["display_url"]

            if caption and media_url:
                return caption, media_url, is_video

        # ── 4. Raw key scan ───────────────────────────────────────────
        if not media_url:
            m = cls._RE_VIDEO_URL.search(text)
            if m:
                is_video  = True
                media_url = cls._unescape(m.group(1))
        if not media_url:
            m = cls._RE_DISPLAY_URL.search(text)
            if m:
                media_url = cls._unescape(m.group(1))
        if not caption:
            m = cls._RE_EDGE_CAPTION.search(text)
            if m:
                caption = cls._unescape(m.group(1))

        return caption or None, media_url or None, is_video


# ══════════════════════════════════════════════
#  Core Scraper
# ══════════════════════════════════════════════

class InstagramScraper:

    def __init__(
        self,
        cookies:        List[Dict],
        logger:         DetailedLogger,
        max_concurrent: int = 2,
    ):
        self.cookies        = cookies
        self.logger         = logger
        self.max_concurrent = max_concurrent
        self.user_agents    = [
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

    async def _safe_evaluate(
        self,
        page:    Page,
        script:  str,
        timeout: float = 10.0,
        label:   str   = "evaluate",
    ) -> Any:
        """page.evaluate() with an asyncio hard timeout (Playwright has none)."""
        try:
            return await asyncio.wait_for(page.evaluate(script), timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.warning(
                f"[{label}] page.evaluate timed out after {timeout:.0f}s — skipping",
                indent=3,
            )
            return None
        except Exception as e:
            self.logger.debug(
                f"[{label}] error: {type(e).__name__}: {str(e).split(chr(10))[0][:80]}",
                indent=3,
            )
            return None

    # ------------------------------------------------------------------
    #  Navigation
    # ------------------------------------------------------------------

    async def safe_goto(self, page: Page, url: str, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            self.logger.debug(
                f"goto attempt {attempt + 1}/{max_retries}  url={url[:80]}", indent=2
            )
            try:
                response = await page.goto(
                    url, wait_until=GOTO_WAIT_UNTIL, timeout=GOTO_TIMEOUT_MS
                )
                if response is None:
                    await asyncio.sleep(1.5)
                    continue

                status = response.status
                self.logger.debug(f"HTTP {status} received", indent=2)

                if status == 429:
                    wait = 5 + attempt * 3
                    self.logger.warning(f"Rate-limited — waiting {wait}s", indent=2)
                    await asyncio.sleep(wait)
                    continue
                if status >= 500:
                    await asyncio.sleep(2 + attempt)
                    continue
                if status < 400:
                    await self._wait_for_dom(page)
                    self.logger.debug(f"Page ready after attempt {attempt + 1}", indent=2)
                    return True

                self.logger.error(f"Non-retriable HTTP {status}", indent=2)
                return False

            except PlaywrightError as e:
                short_err = str(e).split("\n")[0][:120]
                self.logger.warning(f"Attempt {attempt + 1} failed — {short_err}", indent=2)
                is_timeout = "Timeout" in str(e) or "timeout" in str(e)
                is_net     = "net::ERR_" in str(e)
                if (is_timeout or is_net) and attempt < max_retries - 1:
                    await asyncio.sleep(2.0 * (attempt + 1) + random.uniform(0, 1))
                    continue
                raise

        self.logger.error(
            f"Navigation failed after {max_retries} attempts for {url[:70]}", indent=1
        )
        return False

    async def _wait_for_dom(self, page: Page, timeout: float = 6.0):
        deadline = asyncio.get_event_loop().time() + timeout
        for sel in ["article", "video", "main", "section", "div[role='main']", "body > div > div"]:
            remaining_ms = (deadline - asyncio.get_event_loop().time()) * 1000
            if remaining_ms <= 200:
                break
            try:
                await page.wait_for_selector(
                    sel, state="attached", timeout=min(remaining_ms, 2_000)
                )
                self.logger.debug(f"DOM ready — matched '{sel}'", indent=2)
                await self._safe_evaluate(
                    page, "window.scrollBy(0,80); window.scrollBy(0,-80)",
                    timeout=2.0, label="scroll-jitter",
                )
                return
            except Exception:
                pass

        elapsed = timeout - (deadline - asyncio.get_event_loop().time())
        self.logger.debug(
            f"DOM selectors did not match in {elapsed:.1f}s — proceeding anyway", indent=2
        )
        await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    #  Popup dismissal
    # ------------------------------------------------------------------

    async def dismiss_popups(self, page: Page):
        for sel in [
            'button:has-text("Not now")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept")',
            '[role="dialog"] button:has-text("Not Now")',
            'button:has-text("Allow essential and optional cookies")',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=800):
                    await el.click(timeout=800)
                    await asyncio.sleep(0.15)
            except Exception:
                pass

    # ------------------------------------------------------------------
    #  JS-based caption / media fallbacks
    # ------------------------------------------------------------------

    async def _js_extract_caption(self, page: Page, shortcode: str) -> Optional[str]:
        """
        JS fallback strategies — only used when HTML body parse yields nothing.

        Key differences from the original version:
        ─────────────────────────────────────────
        1. GraphQL uses fetch() with credentials:'include' instead of XHR
           with manually-set headers.  credentials:'include' auto-attaches
           ALL browser cookies (including httpOnly ones JS can't read),
           making the request indistinguishable from the browser's own calls.

        2. Window globals checks __additionalDataLoaded (correct name) and
           __additionalData (legacy fallback), plus _sharedData for old posts.
        """
        # ── Strategy 1: GraphQL via fetch with credentials:include ────
        # Use JSON.stringify to safely embed variables — avoids shell-escaping
        # issues that plagued the old body.replace("'", "\\'") approach.
        gql_script = f"""
            async () => {{
                try {{
                    const vars = {{
                        shortcode: {json.dumps(shortcode)},
                        fetch_tagged_user_count: null,
                        hoisted_comment_id: null,
                        hoisted_reply_id: null
                    }};
                    const body = 'variables=' + encodeURIComponent(JSON.stringify(vars))
                               + '&doc_id={self.post_doc_id}';

                    const resp = await fetch('https://www.instagram.com/graphql/query', {{
                        method: 'POST',
                        headers: {{
                            'content-type': 'application/x-www-form-urlencoded',
                            'x-csrftoken': {json.dumps(self.csrf_token or '')},
                            'x-ig-app-id': '{self.ig_app_id}',
                            'x-requested-with': 'XMLHttpRequest'
                        }},
                        credentials: 'include',
                        body: body
                    }});

                    if (!resp.ok) return {{_err: 'http:' + resp.status}};
                    return await resp.json();
                }} catch (e) {{
                    return {{_err: e.message}};
                }}
            }}
        """
        result = await self._safe_evaluate(page, gql_script, timeout=12.0, label="graphql-fetch")
        if result and not result.get("_err"):
            media = result.get("data", {}).get("xdt_shortcode_media", {})
            if media:
                edges = media.get("edge_media_to_caption", {}).get("edges", [])
                cap = edges[0]["node"].get("text") if edges else media.get("accessibility_caption")
                if cap:
                    self.logger.debug(f"Caption via [GraphQL fetch]  {len(cap)} chars", indent=2)
                    return cap
        elif result and result.get("_err"):
            self.logger.debug(f"GraphQL fetch: {result['_err']}", indent=3)

        # ── Strategy 2: Window globals ────────────────────────────────
        # Checks __additionalDataLoaded (current), __additionalData (legacy),
        # and _sharedData (very old posts).
        window_script = r"""
            () => {
                // __additionalDataLoaded — current Instagram global (2024+)
                try {
                    const dl = window.__additionalDataLoaded || {};
                    for (const key of Object.keys(dl)) {
                        const d = dl[key];
                        const m = d?.data?.xdt_shortcode_media
                               || d?.data?.shortcode_media
                               || d?.graphql?.shortcode_media;
                        if (m) {
                            const e = m.edge_media_to_caption?.edges || [];
                            if (e.length) return e[0].node.text;
                        }
                    }
                } catch (_) {}

                // __additionalData — older global (2022-2023)
                try {
                    const ad = window.__additionalData || {};
                    for (const key of Object.keys(ad)) {
                        const m = ad[key]?.data?.xdt_shortcode_media
                               || ad[key]?.data?.shortcode_media;
                        if (m) {
                            const e = m.edge_media_to_caption?.edges || [];
                            if (e.length) return e[0].node.text;
                        }
                    }
                } catch (_) {}

                // _sharedData — legacy global (pre-2022)
                try {
                    const m = window._sharedData?.entry_data?.PostPage?.[0]
                                ?.graphql?.shortcode_media;
                    if (m) {
                        const e = m.edge_media_to_caption?.edges || [];
                        if (e.length) return e[0].node.text;
                    }
                } catch (_) {}

                return null;
            }
        """
        cap = await self._safe_evaluate(page, window_script, timeout=4.0, label="window-globals")
        if cap:
            self.logger.debug(f"Caption via [window globals]  {len(cap)} chars", indent=2)
            return cap

        # ── Strategy 3: DOM selectors ─────────────────────────────────
        dom_script = r"""
            () => {
                for (const s of [
                    'div._aacl._a9zr._a9zo._a9z9',
                    'div._aacl._a9zr',
                    'div[data-testid="post-caption"]',
                    'h1 + div span',
                    'span._aacl'
                ]) {
                    const el = document.querySelector(s);
                    const t  = el?.innerText?.trim();
                    if (t && t.length > 5) return t;
                }
                return null;
            }
        """
        cap = await self._safe_evaluate(page, dom_script, timeout=4.0, label="dom-caption")
        if cap:
            self.logger.debug(f"Caption via [DOM selectors]  {len(cap)} chars", indent=2)
            return cap

        return None

    async def _js_extract_media(self, page: Page, post_url: str) -> Tuple[str, bool]:
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
                        const dl = window.__additionalDataLoaded || {};
                        for (const k of Object.keys(dl)) {
                            const m = dl[k]?.data?.xdt_shortcode_media;
                            if (m?.video_url) return m.video_url;
                        }
                        for (const k of Object.keys(window.__additionalData || {})) {
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
                        .filter(i =>
                            (i.src.includes('cdninstagram') || i.src.includes('fbcdn'))
                            && !i.src.includes('profile') && i.naturalWidth > 300)
                        .sort((a, b) => b.naturalWidth - a.naturalWidth);
                    return cands[0]?.src || '';
                }
            """
        url = await self._safe_evaluate(page, script, timeout=6.0, label="media-url")
        return (url if url and not url.startswith("blob") else ""), is_video

    # ------------------------------------------------------------------
    #  Single post scraper — three-layer strategy
    # ------------------------------------------------------------------

    async def scrape_single_post(
        self,
        context:    BrowserContext,
        post_url:   str,
        shortcode:  str,
        post_index: int,
    ) -> ScrapingResult:
        """
        Three-layer data extraction strategy, in priority order:

        LAYER 1 — Network-layer capture + Python parse (primary)
        ─────────────────────────────────────────────────────────
        A response listener is registered before navigation.  It captures:

          (a) The raw HTML bytes of the main document response
              → parsed by InstagramHtmlParser in pure Python
              → covers pages where data is embedded inline in <script> tags
                (dominant pattern for Instagram reels / older posts)

          (b) Any JSON API responses (graphql / v1 media endpoints)
              → parsed directly as dicts in Python
              → covers pages that fetch data via XHR after initial load

        Neither (a) nor (b) touches the Chromium JS runtime — both work
        correctly even when the JS thread is saturated.

        LAYER 2 — JS evaluate fallback
        ────────────────────────────────
        Only reached when Layer 1 yields no media_url.

          • GraphQL via fetch() with credentials:'include'
            (auto-sends all cookies including httpOnly — more reliable
            than manually setting headers)
          • window.__additionalDataLoaded  (correct 2024+ global)
          • window.__additionalData        (legacy 2022-2023 global)
          • window._sharedData             (very old posts)
          • DOM selector scan

        LAYER 3 — Failure
        ──────────────────
        Return ScrapingResult(success=False).
        """
        page      = None
        t0        = time.monotonic()
        post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"

        html_body_ref: list = [None]   # bytes — main page HTML
        api_data_ref:  list = [None]   # dict  — first matching JSON API response
        api_ready           = asyncio.Event()

        is_video_default = any(p in post_url for p in SLOW_PATH_PATTERNS)

        self.logger.info(f"[{post_index:>2}] {post_type}  {shortcode}", indent=1)

        # ── Response listener ─────────────────────────────────────────
        async def on_response(response):
            url = response.url

            # Capture main page HTML
            if html_body_ref[0] is None and response.request.resource_type == "document":
                try:
                    if response.status == 200:
                        body = await asyncio.wait_for(response.body(), timeout=8.0)
                        html_body_ref[0] = body
                        self.logger.debug(
                            f"[{post_index}] HTML captured  {len(body):,} bytes", indent=2
                        )
                except Exception:
                    pass
                return

            # Capture JSON API responses
            if api_data_ref[0] is not None:
                return
            if not any(sig in url for sig in ("graphql/query", "/api/v1/media/", "/api/graphql")):
                return
            try:
                if response.status != 200:
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                data = await asyncio.wait_for(response.json(), timeout=3.0)
                api_data_ref[0] = data
                api_ready.set()
            except Exception:
                pass

        # ── Page setup ────────────────────────────────────────────────
        try:
            page = await context.new_page()
            page.set_default_navigation_timeout(40_000)
            page.set_default_timeout(15_000)
            await page.route("**/*", smart_route_handler)

            page.on("response", on_response)

            # ── Navigate ──────────────────────────────────────────────
            self.logger.debug(f"[{post_index}] navigating …", indent=2)
            if not await self.safe_goto(page, post_url, max_retries=3):
                return ScrapingResult(success=False, error="Navigation failed after all retries")

            if "accounts/login" in page.url:
                self.logger.error(
                    f"[{post_index}] redirected to login — session expired", indent=2
                )
                return ScrapingResult(success=False, error="Redirected to login")

            await self.dismiss_popups(page)

            # Brief window for any post-load API XHRs
            try:
                await asyncio.wait_for(api_ready.wait(), timeout=5.0)
                self.logger.debug(f"[{post_index}] API JSON intercepted", indent=2)
            except asyncio.TimeoutError:
                self.logger.debug(
                    f"[{post_index}] no API JSON in 5s — relying on HTML parse", indent=2
                )

            # ── LAYER 1 ───────────────────────────────────────────────
            caption:   Optional[str] = None
            media_url: Optional[str] = None
            is_video:  bool          = is_video_default

            # 1a — JSON API response
            if api_data_ref[0]:
                api   = api_data_ref[0]
                media = (
                    api.get("data", {}).get("xdt_shortcode_media")
                    or api.get("data", {}).get("shortcode_media")
                    or api.get("graphql", {}).get("shortcode_media")
                )
                if not media:
                    items = api.get("items") or []
                    if items:
                        item  = items[0]
                        cap   = item.get("caption") or {}
                        caption = cap.get("text") if isinstance(cap, dict) else None
                        if item.get("video_versions"):
                            is_video  = True
                            media_url = item["video_versions"][0].get("url")
                        elif item.get("image_versions2"):
                            cands = item["image_versions2"].get("candidates", [])
                            media_url = cands[0].get("url") if cands else None
                else:
                    edges = media.get("edge_media_to_caption", {}).get("edges", [])
                    if edges:
                        caption = edges[0].get("node", {}).get("text")
                    if media.get("video_url"):
                        is_video  = True
                        media_url = media["video_url"]
                    elif media.get("display_url"):
                        media_url = media["display_url"]

                if media_url:
                    self.logger.debug(
                        f"[{post_index}] Layer 1a (API JSON) ✓", indent=2
                    )

            # 1b — HTML body parse
            if not media_url and html_body_ref[0]:
                self.logger.debug(
                    f"[{post_index}] Layer 1b — HTML parse "
                    f"({len(html_body_ref[0]):,} bytes) …",
                    indent=2,
                )
                h_cap, h_url, h_vid = InstagramHtmlParser.parse(
                    html_body_ref[0], shortcode, is_video
                )
                if h_url:
                    media_url = h_url
                    is_video  = h_vid
                    self.logger.debug(
                        f"[{post_index}] Layer 1b (HTML parse) ✓", indent=2
                    )
                if not caption and h_cap:
                    caption = h_cap

            # ── LAYER 2: JS evaluate fallback ─────────────────────────
            if not media_url:
                self.logger.warning(
                    f"[{post_index}] Layers 1a+1b empty — trying JS fallback",
                    indent=2,
                )
                if not caption:
                    caption = await self._js_extract_caption(page, shortcode) or ""
                media_url, is_video = await self._js_extract_media(page, post_url)

            # ── Result ────────────────────────────────────────────────
            elapsed = time.monotonic() - t0

            if media_url and not media_url.startswith("blob"):
                kind     = "VIDEO" if is_video else "IMAGE"
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
                        "caption":   caption or "",
                        "media_url": media_url,
                        "is_video":  is_video,
                    },
                )

            self.logger.warning(
                f"[{post_index:>2}] no media URL found  "
                f"shortcode={shortcode}  {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(success=False, error="No media URL found")

        except Exception as e:
            elapsed = time.monotonic() - t0
            self.logger.error(
                f"[{post_index:>2}] {type(e).__name__}: "
                f"{str(e).split(chr(10))[0][:80]}  {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(success=False, error=str(e)[:80])

        finally:
            if page:
                try:
                    page.remove_listener("response", on_response)
                except Exception:
                    pass
                try:
                    await page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    #  Profile scraper (scroll + collect post URLs)
    # ------------------------------------------------------------------

    async def _collect_post_urls(self, page: Page, post_limit: int) -> List[str]:
        post_urls:    List[str] = []
        last_height:  int       = 0
        stale_rounds: int       = 0
        MAX_STALE   = 3
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
            links = (
                await self._safe_evaluate(page, js_collect, timeout=5.0, label="collect-links")
                or []
            )
            new = [u for u in links if u not in post_urls]
            post_urls.extend(new)

            if new:
                self.logger.info(
                    f"Scroll {i+1:>2}/{MAX_SCROLLS}  "
                    f"+{len(new)} new  →  {len(post_urls)} total",
                    indent=2,
                )
            else:
                self.logger.debug(
                    f"Scroll {i+1:>2}/{MAX_SCROLLS}  no new  "
                    f"(stale {stale_rounds + 1}/{MAX_STALE})",
                    indent=2,
                )

            if len(post_urls) >= post_limit:
                self.logger.info(
                    f"Post limit ({post_limit}) reached — stopping scroll", indent=2
                )
                break

            await self._safe_evaluate(
                page, "window.scrollTo(0, document.body.scrollHeight)",
                timeout=3.0, label="scroll",
            )
            for _ in range(4):
                await asyncio.sleep(0.5)
                new_height = (
                    await self._safe_evaluate(
                        page, "document.body.scrollHeight",
                        timeout=3.0, label="scroll-height",
                    )
                    or last_height
                )
                if new_height != last_height:
                    last_height  = new_height
                    stale_rounds = 0
                    break
            else:
                stale_rounds += 1

            if stale_rounds >= MAX_STALE:
                self.logger.info(
                    f"Page end — no new content after {MAX_STALE} consecutive scrolls",
                    indent=2,
                )
                break

        return post_urls[:post_limit]

    # ------------------------------------------------------------------
    #  Main entry point
    # ------------------------------------------------------------------

    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
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
                        "Redirected to login — cookies are expired or invalid", indent=1
                    )
                    await profile_page.close()
                    return []

                self.logger.success(f"Profile loaded: {profile_page.url[:70]}", indent=2)
                self.logger.section_end()

                await self.dismiss_popups(profile_page)
                await self._wait_for_dom(profile_page)

                # ── Phase: Discover posts ─────────────────────────────
                self.logger.phase(
                    "Discover Posts", f"Scrolling grid — target: {post_limit} posts"
                )
                self.logger.section("Grid scroll")
                post_urls = await self._collect_post_urls(profile_page, post_limit)
                await profile_page.close()
                self.logger.section_end(f"{len(post_urls)} unique post URLs collected")

                if not post_urls:
                    self.logger.error(
                        "No post URLs found — profile may be private or empty", indent=1
                    )
                    return []

                self.logger.separator()
                for i, u in enumerate(post_urls, 1):
                    sc = (
                        u.split("/p/")[-1]
                         .split("/reel/")[-1]
                         .split("/tv/")[-1]
                         .split("/")[0]
                    )
                    self.logger.debug(f"  {i:>2}.  {sc:<20}  {u}", indent=1)
                self.logger.separator()

                # ── Phase: Scrape posts ───────────────────────────────
                self.logger.phase(
                    "Scrape Posts",
                    f"{len(post_urls)} posts  ·  {self.max_concurrent} concurrent workers",
                )

                self.logger.section("Worker contexts")
                worker_contexts: List[BrowserContext] = []
                for _ in range(self.max_concurrent):
                    wctx = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        viewport={"width": 1920, "height": 1080},
                        locale="en-US",
                        ignore_https_errors=True,
                    )
                    await wctx.add_cookies(self.cookies)
                    worker_contexts.append(wctx)
                self.logger.success(f"{self.max_concurrent} worker contexts ready", indent=2)
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
                        failures.append(
                            f"  post {i+1}: {type(res).__name__}: {str(res)[:60]}"
                        )

                for wctx in worker_contexts:
                    try:
                        await wctx.close()
                    except Exception:
                        pass

                # ── Final summary ─────────────────────────────────────
                elapsed_total = time.monotonic() - t_total
                self.logger.phase("Summary", f"Total time: {elapsed_total:.1f}s")
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

                videos    = sum(1 for p in posts if p.get("is_video"))
                images    = len(posts) - videos
                captioned = sum(1 for p in posts if p.get("caption"))
                self.logger.info(f"Images:       {images}", indent=1)
                self.logger.info(f"Videos:       {videos}", indent=1)
                self.logger.info(f"With caption: {captioned}/{len(posts)}", indent=1)
                self.logger.separator()
                self.logger.progress(len(posts), len(post_urls), "posts scraped")

                return posts

            except Exception as e:
                import traceback
                self.logger.error(
                    f"Fatal error: {type(e).__name__}: {str(e)[:80]}", indent=1
                )
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
    account:        str,
    cookies:        List[Dict[str, Any]] = None,
    max_concurrent: int = 2,
) -> List[Dict[str, Any]]:
    """
    Scrape an Instagram profile and return post data.

    Returns:
        List of dicts: url, shortcode, caption, media_url, is_video

    max_concurrent=2 is the safe default for Render's free tier.
    Drop to 1 if you still see JS timeouts in the fallback path.
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

    csrf_ok    = any(c.get("name") == "csrftoken" for c in cookies)
    session_ok = any(c.get("name") == "sessionid" for c in cookies)
    logger.info(
        f"csrftoken present: {csrf_ok}  |  sessionid present: {session_ok}", indent=2
    )
    if not csrf_ok:
        logger.warning("csrftoken missing — GraphQL JS fallback will likely fail", indent=2)
    if not session_ok:
        logger.warning(
            "sessionid missing — profile may not load as authenticated", indent=2
        )

    logger.section_end()

    scraper = InstagramScraper(
        cookies=cookies, logger=logger, max_concurrent=max_concurrent
    )
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )