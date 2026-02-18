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
#  Config guard
# ─────────────────────────────────────────────
try:
    from Utils import config
except ImportError:
    class config:
        POST_LIMIT = 10


# ══════════════════════════════════════════════
#  CAPTION-FOCUSED CONFIGURATION
# ══════════════════════════════════════════════

GOTO_TIMEOUT_MS = 60_000  # 60 seconds
GOTO_WAIT_UNTIL = "commit"

# Shorter timeouts since we're only extracting captions
SAFE_EVALUATE_TIMEOUT = 30.0  # 30 seconds (was 40s)

CDN_ALLOWLIST = ("cdninstagram", "fbcdn")
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

_BLOCK_TYPES = frozenset({"font", "stylesheet"})
_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net",
)


# ══════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════

class DetailedLogger:
    _ICONS = {
        "info": "·",
        "success": "✓",
        "warning": "⚠",
        "error": "✗",
        "debug": "…",
    }

    def __init__(self, name: str = "IG Scraper"):
        self.name = name
        self._start_ts = time.monotonic()
        self._phase_ts = self._start_ts
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
        self._phase_ts = time.monotonic()
        W = 60
        elapsed = self._elapsed()
        header = f"  PHASE {self._phase_num} · {title}"
        padding = W - len(header) - len(elapsed) - 2
        self._emit(logging.INFO, "")
        self._emit(logging.INFO, "╔" + "═" * W + "╗")
        self._emit(logging.INFO, f"║{header}{' ' * max(padding, 1)}{elapsed}  ║")
        if subtitle:
            self._emit(logging.INFO, f"║  {subtitle[:W-2]:<{W-2}}║")
        self._emit(logging.INFO, "╚" + "═" * W + "╝")

    def section(self, title: str):
        ts = self._ts()
        line = f"  ├─ [{ts}] {title} "
        self._emit(logging.INFO, line + "─" * max(0, 64 - len(line)))

    def section_end(self, summary: str = ""):
        parts = [f"  └─ done in {self._phase_elapsed()}"]
        if summary:
            parts.append(f"  ·  {summary}")
        self._emit(logging.INFO, "".join(parts))

    def info(self, msg: str, indent: int = 1):
        self._emit(logging.INFO, f"{'     ' * indent}{self._ICONS['info']}  {msg}")

    def success(self, msg: str, indent: int = 1):
        self._emit(logging.INFO, f"{'     ' * indent}{self._ICONS['success']}  {msg}")

    def warning(self, msg: str, indent: int = 1):
        self._emit(logging.WARNING, f"{'     ' * indent}{self._ICONS['warning']}  {msg}")

    def error(self, msg: str, indent: int = 1):
        self._emit(logging.ERROR, f"{'     ' * indent}{self._ICONS['error']}  {msg}")

    def debug(self, msg: str, indent: int = 1):
        self._emit(logging.DEBUG, f"{'     ' * indent}{self._ICONS['debug']}  {msg}")

    def progress(self, done: int, total: int, label: str = ""):
        bar_w = 10
        filled = round(bar_w * done / max(total, 1))
        bar = "▓" * filled + "░" * (bar_w - filled)
        suffix = f"  {label}" if label else ""
        self._emit(logging.INFO, f"       [{bar}]  {done}/{total}{suffix}")

    def separator(self):
        self._emit(logging.INFO, "  " + "─" * 62)


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
    try:
        url = route.request.url
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
#  CAPTION-FOCUSED HTML Parser
# ══════════════════════════════════════════════

class InstagramCaptionParser:
    """
    Caption-focused HTML parser with multiple fallback strategies.
    
    Priority order (most reliable first):
    1. JSON-LD structured data
    2. og:description meta tag
    3. shortcode_media JSON blob (inline script)
    4. edge_media_to_caption pattern
    5. Direct caption text patterns
    """

    @classmethod
    def _unescape(cls, s: str) -> str:
        """Unescape JSON/HTML entities."""
        try:
            return json.loads(f'"{s}"')
        except Exception:
            return s.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")

    @classmethod
    def _find_json_object(cls, text: str, start: int) -> Optional[str]:
        """Extract complete JSON object starting at position."""
        depth = 0
        in_str = False
        escape = False
        i = start
        n = len(text)
        
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
    def parse(cls, html: bytes, shortcode: str) -> Optional[str]:
        """
        Parse caption from HTML bytes.
        Returns caption string or None if not found.
        """
        try:
            text = html.decode("utf-8", errors="ignore")
        except Exception:
            return None

        # ── Strategy 1: JSON-LD structured data ───────────────────────
        # Most reliable source - Instagram's structured data
        jsonld_pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.I,
        )
        
        for match in jsonld_pattern.finditer(text):
            try:
                blob = json.loads(match.group(1))
                if isinstance(blob, list):
                    blob = blob[0] if blob else {}
                
                # Try multiple caption fields
                caption = (
                    blob.get("caption") or
                    blob.get("description") or
                    blob.get("articleBody") or
                    ""
                )
                
                if caption and len(caption) > 5:
                    return caption.strip()
            except Exception:
                pass

        # ── Strategy 2: og:description meta tag ───────────────────────
        # Second most reliable - OpenGraph metadata
        og_desc_pattern = re.compile(
            r'<meta[^>]+(?:property=["\']og:description["\']|name=["\']description["\'])'
            r'[^>]+content=["\']([^"\']{10,})["\']',
            re.I
        )
        
        match = og_desc_pattern.search(text)
        if match:
            raw = cls._unescape(match.group(1))
            # Clean up Instagram-specific prefixes
            cleaned = re.sub(r'^[^:]+:\s*', "", raw).strip()
            cleaned = re.sub(r'^\d+\s+(?:Likes?|Comments?|Views?)[,\s]*', "", cleaned, flags=re.I)
            if len(cleaned) > 10:
                return cleaned

        # ── Strategy 3: shortcode_media JSON blob ─────────────────────
        # Inline JavaScript data object
        shortcode_media_pattern = re.compile(
            r'"(?:shortcode_media|xdt_shortcode_media)"\s*:\s*(\{)',
        )
        
        for match in shortcode_media_pattern.finditer(text):
            blob_str = cls._find_json_object(text, match.start(1))
            if not blob_str:
                continue
            
            try:
                media = json.loads(blob_str)
                
                # Try edge_media_to_caption first
                edges = media.get("edge_media_to_caption", {}).get("edges", [])
                if edges:
                    caption = edges[0].get("node", {}).get("text")
                    if caption:
                        return caption.strip()
                
                # Try accessibility_caption as fallback
                accessibility = media.get("accessibility_caption")
                if accessibility and len(accessibility) > 10:
                    return accessibility.strip()
                    
            except Exception:
                pass

        # ── Strategy 4: edge_media_to_caption pattern ─────────────────
        # Direct regex for caption edges
        edge_caption_pattern = re.compile(
            r'"edge_media_to_caption"\s*:\s*\{[^}]*"edges"\s*:\s*\[\s*\{[^}]*"node"\s*:\s*'
            r'\{[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
            re.DOTALL,
        )
        
        match = edge_caption_pattern.search(text)
        if match:
            caption = cls._unescape(match.group(1))
            if len(caption) > 5:
                return caption.strip()

        # ── Strategy 5: Direct caption text patterns ──────────────────
        # Look for common caption patterns in HTML structure
        caption_patterns = [
            # Pattern 1: "caption":"text"
            r'"caption"\s*:\s*"((?:[^"\\]|\\.){10,})"',
            # Pattern 2: {"text":"caption text"}
            r'\{"text"\s*:\s*"((?:[^"\\]|\\.){10,})"\}',
            # Pattern 3: "articleBody":"text"
            r'"articleBody"\s*:\s*"((?:[^"\\]|\\.){10,})"',
        ]
        
        for pattern in caption_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                caption = cls._unescape(match.group(1))
                # Filter out non-caption text (too short or looks like code)
                if len(caption) > 10 and not re.match(r'^[\w_]+$', caption):
                    return caption.strip()

        # No caption found
        return None


# ══════════════════════════════════════════════
#  CAPTION-FOCUSED Scraper
# ══════════════════════════════════════════════

class InstagramCaptionScraper:
    """
    Caption-focused scraper optimized for Render free tier.
    
    Returns only URL and caption (no media_url, no is_video).
    Much lighter and faster than full scraper.
    """

    def __init__(
        self,
        cookies: List[Dict],
        logger: DetailedLogger,
    ):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
        self.csrf_token = next(
            (c["value"] for c in cookies if c.get("name") == "csrftoken"), None
        )

    async def _human_delay(self, min_ms: int = 800, max_ms: int = 2000):
        """Conservative delays."""
        await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)

    async def _safe_evaluate(
        self,
        page: Page,
        script: str,
        timeout: float = SAFE_EVALUATE_TIMEOUT,
        label: str = "evaluate",
    ) -> Any:
        """Ultra-safe evaluate with error handling."""
        try:
            return await asyncio.wait_for(page.evaluate(script), timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.warning(
                f"[{label}] page.evaluate timed out after {timeout:.0f}s — skipping",
                indent=3,
            )
            return None
        except PlaywrightError as e:
            if "destroyed" not in str(e).lower():
                self.logger.debug(
                    f"[{label}] error: {type(e).__name__}: {str(e)[:80]}",
                    indent=3,
                )
            return None
        except Exception as e:
            self.logger.debug(
                f"[{label}] unexpected error: {type(e).__name__}: {str(e)[:80]}",
                indent=3,
            )
            return None

    async def safe_goto(self, page: Page, url: str, max_retries: int = 3) -> bool:
        """Navigation with challenge detection."""
        for attempt in range(max_retries):
            self.logger.debug(
                f"goto attempt {attempt + 1}/{max_retries}  url={url[:80]}", indent=2
            )
            try:
                response = await page.goto(
                    url, wait_until=GOTO_WAIT_UNTIL, timeout=GOTO_TIMEOUT_MS
                )
                if response is None:
                    await asyncio.sleep(2.0)
                    continue

                status = response.status
                self.logger.debug(f"HTTP {status} received", indent=2)

                if status == 429:
                    wait = 10 + attempt * 5
                    self.logger.warning(f"Rate-limited — waiting {wait}s", indent=2)
                    await asyncio.sleep(wait)
                    continue
                if status >= 500:
                    await asyncio.sleep(3 + attempt * 2)
                    continue
                if status < 400:
                    # Check for challenge/checkpoint
                    current_url = page.url
                    if "challenge" in current_url or "checkpoint" in current_url:
                        self.logger.error(
                            f"Challenge/checkpoint page: {current_url[:70]}",
                            indent=2
                        )
                        return False
                    
                    await self._wait_for_dom(page)
                    self.logger.debug(f"Page ready after attempt {attempt + 1}", indent=2)
                    return True

                self.logger.error(f"Non-retriable HTTP {status}", indent=2)
                return False

            except PlaywrightError as e:
                short_err = str(e).split("\n")[0][:120]
                self.logger.warning(f"Attempt {attempt + 1} failed — {short_err}", indent=2)
                is_timeout = "Timeout" in str(e) or "timeout" in str(e)
                is_net = "net::ERR_" in str(e)
                if (is_timeout or is_net) and attempt < max_retries - 1:
                    await asyncio.sleep(3.0 * (attempt + 1) + random.uniform(0, 2))
                    continue
                raise

        self.logger.error(
            f"Navigation failed after {max_retries} attempts for {url[:70]}", indent=1
        )
        return False

    async def _wait_for_dom(self, page: Page, timeout: float = 10.0):
        """Longer DOM wait for slow servers."""
        deadline = asyncio.get_event_loop().time() + timeout
        for sel in ["article", "video", "main", "section", "div[role='main']", "body > div"]:
            remaining_ms = (deadline - asyncio.get_event_loop().time()) * 1000
            if remaining_ms <= 300:
                break
            try:
                await page.wait_for_selector(
                    sel, state="attached", timeout=min(remaining_ms, 4_000)
                )
                self.logger.debug(f"DOM ready — matched '{sel}'", indent=2)
                return
            except Exception:
                pass

        elapsed = timeout - (deadline - asyncio.get_event_loop().time())
        self.logger.debug(
            f"DOM selectors did not match in {elapsed:.1f}s — proceeding anyway", indent=2
        )
        await asyncio.sleep(0.5)

    async def dismiss_popups(self, page: Page):
        """Dismiss popups with longer delays."""
        for sel in [
            'button:has-text("Not now")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept")',
            '[role="dialog"] button',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1000):
                    await el.click(timeout=1000)
                    await self._human_delay(200, 500)
            except Exception:
                pass

    async def _js_extract_caption_fallback(
        self,
        page: Page,
        shortcode: str,
    ) -> Optional[str]:
        """
        Minimal JS fallback for caption extraction.
        Only used if HTML parsing fails completely.
        """
        # Try window globals (fastest)
        window_script = r"""
            () => {
                try {
                    // __additionalDataLoaded (current)
                    const dl = window.__additionalDataLoaded || {};
                    for (const key of Object.keys(dl)) {
                        const d = dl[key];
                        const m = d?.data?.xdt_shortcode_media
                               || d?.data?.shortcode_media
                               || d?.graphql?.shortcode_media;
                        if (m) {
                            const e = m.edge_media_to_caption?.edges || [];
                            if (e.length) return e[0].node.text;
                            if (m.accessibility_caption) return m.accessibility_caption;
                        }
                    }
                    
                    // __additionalData (legacy)
                    const ad = window.__additionalData || {};
                    for (const key of Object.keys(ad)) {
                        const m = ad[key]?.data?.xdt_shortcode_media
                               || ad[key]?.data?.shortcode_media;
                        if (m) {
                            const e = m.edge_media_to_caption?.edges || [];
                            if (e.length) return e[0].node.text;
                        }
                    }
                    
                    // _sharedData (very old)
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
        
        cap = await self._safe_evaluate(
            page, window_script, 
            timeout=15.0,  # Shorter timeout
            label="window-globals-caption"
        )
        if cap:
            self.logger.debug(f"Caption via [JS fallback]  {len(cap)} chars", indent=2)
            return cap

        # Try DOM selectors (slower but sometimes works)
        dom_script = r"""
            () => {
                const selectors = [
                    'div._aacl._a9zr._a9zo._a9z9',
                    'div._aacl._a9zr',
                    'div[data-testid="post-caption"]',
                    'h1 + div span',
                    'span._aacl',
                    'article span[dir="auto"]',
                ];
                
                for (const s of selectors) {
                    const el = document.querySelector(s);
                    const t = el?.innerText?.trim();
                    if (t && t.length > 5) return t;
                }
                return null;
            }
        """
        
        cap = await self._safe_evaluate(
            page, dom_script, 
            timeout=15.0,
            label="dom-caption"
        )
        if cap:
            self.logger.debug(f"Caption via [DOM fallback]  {len(cap)} chars", indent=2)
            return cap

        return None

    async def scrape_single_post(
        self,
        page: Page,
        post_url: str,
        shortcode: str,
        post_index: int,
    ) -> ScrapingResult:
        """
        Caption-focused scraping with fallback.
        
        Strategy:
        1. HTML parse (primary - most reliable)
        2. JS fallback (if HTML fails)
        
        Returns: url, shortcode, caption
        """
        t0 = time.monotonic()
        post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"

        html_body_ref: list = [None]

        self.logger.info(f"[{post_index:>2}] {post_type}  {shortcode}", indent=1)

        # Response listener
        async def on_response(response):
            if html_body_ref[0] is not None:
                return
            if response.request.resource_type != "document":
                return
            try:
                if response.status == 200:
                    body = await asyncio.wait_for(response.body(), timeout=15.0)
                    html_body_ref[0] = body
                    self.logger.debug(
                        f"[{post_index}] HTML captured  {len(body):,} bytes", indent=2
                    )
            except Exception:
                pass

        try:
            page.on("response", on_response)

            # Navigate
            self.logger.debug(f"[{post_index}] navigating …", indent=2)
            if not await self.safe_goto(page, post_url, max_retries=2):
                return ScrapingResult(success=False, error="Navigation failed")

            # Check for challenge/login redirect
            if "challenge" in page.url or "checkpoint" in page.url:
                return ScrapingResult(success=False, error="Challenge page")
            if "accounts/login" in page.url:
                return ScrapingResult(success=False, error="Redirected to login")

            await self.dismiss_popups(page)

            # Wait for HTML to be captured
            await asyncio.sleep(2.0)

            caption: Optional[str] = None

            # ── Primary: HTML parse ───────────────────────────────────
            if html_body_ref[0]:
                self.logger.debug(
                    f"[{post_index}] HTML parse ({len(html_body_ref[0]):,} bytes) …",
                    indent=2,
                )
                caption = InstagramCaptionParser.parse(html_body_ref[0], shortcode)
                
                if caption:
                    self.logger.debug(
                        f"[{post_index}] HTML parse ✓  {len(caption)} chars", 
                        indent=2
                    )
            else:
                self.logger.warning(f"[{post_index}] No HTML captured", indent=2)

            # ── Fallback: JS extraction ──────────────────────────────
            if not caption:
                self.logger.warning(
                    f"[{post_index}] HTML parse empty — trying JS fallback",
                    indent=2,
                )
                caption = await self._js_extract_caption_fallback(page, shortcode)

            # ── Result ────────────────────────────────────────────────
            elapsed = time.monotonic() - t0

            if caption:
                cap_len = len(caption)
                self.logger.success(
                    f"[{post_index:>2}] ✓ {post_type:<5}  {shortcode:<20}  "
                    f"caption: {cap_len} chars  {elapsed:.1f}s",
                    indent=1,
                )
                return ScrapingResult(
                    success=True,
                    data={
                        "url": post_url,
                        "shortcode": shortcode,
                        "caption": caption.strip(),
                    },
                )

            self.logger.warning(
                f"[{post_index:>2}] no caption found  "
                f"shortcode={shortcode}  {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(
                success=True,  # Still success, just no caption
                data={
                    "url": post_url,
                    "shortcode": shortcode,
                    "caption": "",
                }
            )

        except Exception as e:
            elapsed = time.monotonic() - t0
            self.logger.error(
                f"[{post_index:>2}] {type(e).__name__}: "
                f"{str(e)[:80]}  {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(success=False, error=str(e)[:80])

        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass

    async def _collect_post_urls(self, page: Page, post_limit: int) -> List[str]:
        """Collect post URLs with conservative scrolling."""
        post_urls: List[str] = []
        last_height: int = 0
        stale_rounds: int = 0
        MAX_STALE = 3
        MAX_SCROLLS = 12

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
                await self._safe_evaluate(page, js_collect, timeout=8.0, label="collect-links")
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
                page,
                "window.scrollTo(0, document.body.scrollHeight - 100);",
                timeout=5.0,
                label="scroll",
            )

            # Check for height change
            for _ in range(4):
                await self._human_delay(600, 1000)
                new_height = (
                    await self._safe_evaluate(
                        page, "document.body.scrollHeight",
                        timeout=5.0, label="scroll-height",
                    )
                    or last_height
                )
                if new_height != last_height:
                    last_height = new_height
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

    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        """
        Caption-focused sequential scraping.
        Returns list of {url, shortcode, caption} dicts.
        """
        t_total = time.monotonic()

        self.logger.phase(
            "Instagram Caption Scraper",
            f"@{username}  ·  limit {post_limit} posts  ·  CAPTIONS ONLY",
        )

        async with async_playwright() as p:

            # Browser startup
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
                    "--window-size=1280,720",
                    "--disable-setuid-sandbox",
                    "--ignore-certificate-errors",
                    "--dns-prefetch-disable",
                    "--single-process",
                ],
            )
            self.logger.success("Browser ready", indent=2)
            self.logger.section_end()

            main_ctx = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={"width": 1280, "height": 720},
                locale="en-US",
                timezone_id="America/New_York",
                java_script_enabled=True,
                ignore_https_errors=True,
            )
            await main_ctx.add_cookies(self.cookies)
            self.logger.debug(f"Session loaded with {len(self.cookies)} cookies", indent=2)

            try:
                # Load profile
                self.logger.phase("Load Profile", f"https://www.instagram.com/{username}/")
                self.logger.section("Navigation")
                profile_page = await main_ctx.new_page()
                await profile_page.route("**/*", smart_route_handler)

                profile_url = f"https://www.instagram.com/{username}/"
                if not await self.safe_goto(profile_page, profile_url, max_retries=3):
                    self.logger.error("Could not load profile — aborting", indent=1)
                    return []

                # Check for challenge/login
                if "challenge" in profile_page.url or "checkpoint" in profile_page.url:
                    self.logger.error(
                        "Profile redirected to challenge — cookies expired", indent=1
                    )
                    await profile_page.close()
                    return []
                if "accounts/login" in profile_page.url:
                    self.logger.error(
                        "Redirected to login — cookies invalid", indent=1
                    )
                    await profile_page.close()
                    return []

                self.logger.success(f"Profile loaded: {profile_page.url[:70]}", indent=2)
                self.logger.section_end()

                await self.dismiss_popups(profile_page)
                await self._wait_for_dom(profile_page)

                # Discover posts
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

                # Scrape captions SEQUENTIALLY
                self.logger.phase(
                    "Scrape Captions",
                    f"{len(post_urls)} posts  ·  SEQUENTIAL (captions only)",
                )

                self.logger.section("Sequential scraping")
                posts: List[Dict] = []
                failures: List[str] = []

                # Reuse same page for all posts
                scrape_page = await main_ctx.new_page()
                await scrape_page.route("**/*", smart_route_handler)

                for i, url in enumerate(post_urls, 1):
                    sc = (
                        url.split("/p/")[-1]
                        .split("/reel/")[-1]
                        .split("/tv/")[-1]
                        .split("/")[0]
                    )

                    result = await self.scrape_single_post(scrape_page, url, sc, i)

                    if result.success:
                        posts.append(result.data)
                    else:
                        failures.append(f"  post {i}: {result.error}")

                    # Human-like delay between posts
                    if i < len(post_urls):
                        await self._human_delay(1500, 3000)

                    self.logger.progress(i, len(post_urls), f"{len(posts)} successful")

                # Close scrape page
                try:
                    await scrape_page.close()
                except Exception:
                    pass

                self.logger.section_end()

                # Final summary
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
                    for f in failures[:5]:
                        self.logger.debug(f, indent=2)
                    if len(failures) > 5:
                        self.logger.debug(f"  ... and {len(failures) - 5} more", indent=2)

                captioned = sum(1 for p in posts if p.get("caption"))
                empty = len(posts) - captioned
                self.logger.info(f"With caption: {captioned}/{len(posts)}", indent=1)
                self.logger.info(f"Empty:        {empty}/{len(posts)}", indent=1)
                
                # Caption stats
                if captioned > 0:
                    caption_lengths = [len(p["caption"]) for p in posts if p.get("caption")]
                    avg_len = sum(caption_lengths) // len(caption_lengths)
                    max_len = max(caption_lengths)
                    self.logger.info(f"Avg caption:  {avg_len} chars", indent=1)
                    self.logger.info(f"Max caption:  {max_len} chars", indent=1)
                
                self.logger.separator()

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
    account: str,
    cookies: List[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Caption-focused Instagram scraper.
    
    Returns list of dicts with:
    - url: Post URL
    - shortcode: Post shortcode
    - caption: Caption text (or empty string if none)
    
    Does NOT return: media_url, is_video
    
    Much lighter and faster than full scraper.
    Perfect for caption monitoring/analysis.
    """
    account = account.lstrip("@")

    logger.phase(
        "fetch_ig_urls",
        f"account=@{account}  mode=CAPTIONS-ONLY (ultra-light)"
    )
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
    logger.info(
        f"csrftoken present: {csrf_ok}  |  sessionid present: {session_ok}", indent=2
    )
    if not session_ok:
        logger.error(
            "sessionid missing — scraping will fail!", indent=2
        )

    logger.section_end()

    scraper = InstagramCaptionScraper(cookies=cookies, logger=logger)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )