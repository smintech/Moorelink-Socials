import asyncio
import json
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode
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
#  2026 BULLETPROOF CONFIGURATION
# ══════════════════════════════════════════════

NAVIGATION_TIMEOUT = 20_000
ELEMENT_TIMEOUT = 8_000
SCRIPT_TIMEOUT = 6.0

# Cascading wait strategies
WAIT_STRATEGIES = [
    ("commit", 8_000),
    ("domcontentloaded", 12_000),
    ("load", 20_000),
]

CDN_ALLOWLIST = ("cdninstagram", "fbcdn")
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

_BLOCK_TYPES = frozenset({"font", "stylesheet", "image", "media"})
_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net", "googletagmanager",
)

# 2026 GraphQL doc_ids - these change periodically, monitor for updates
# Source: DevTools Network tab when viewing a post
GRAPHQL_DOC_IDS = {
    "post": "8845758582119845",  # PolarisPostActionLoadPostQueryQuery
    "reel": "25981206651899035",  # Reel-specific
    "comments": "24368985919464652",  # Comment pagination
}

# Instagram API headers for 2026
INSTAGRAM_HEADERS = {
    "x-ig-app-id": "936619743392459",  # Web app ID (stable)
    "x-asbd-id": "129477",  # Additional security header
    "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "referer": "https://www.instagram.com/",
}


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
        for noisy in ("playwright", "asyncio", "urllib3"):
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
    """Block unnecessary resources to speed up loading"""
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
    except Exception:
        try:
            await route.abort()
        except:
            pass


# ══════════════════════════════════════════════
#  2026 BULLETPROOF CAPTION EXTRACTOR
# ══════════════════════════════════════════════

class CaptionExtractor2026:
    """
    Multi-strategy caption extractor for 2026 Instagram.
    Priority (most reliable first):
    1. GraphQL API direct call (bypasses HTML parsing entirely)
    2. JSON-LD structured data (schema.org)
    3. Meta tags (og:description, twitter:description)
    4. Hydration data (__additionalDataLoaded)
    5. Inline scripts (window._sharedData)
    6. HTML patterns (last resort)
    """
    
    def __init__(self, logger: DetailedLogger):
        self.logger = logger
    
    # ── Strategy 1: GraphQL API (MOST RELIABLE 2026) ─────────────────
    
    async def extract_graphql(self, shortcode: str, context: BrowserContext) -> Optional[str]:
        """
        Direct GraphQL API call - bypasses HTML entirely.
        Uses Instagram's internal API with proper headers.
        """
        try:
            # Create API page
            api_page = await context.new_page()
            
            # Prepare GraphQL request
            variables = json.dumps({
                "shortcode": shortcode,
                "fetch_tagged_user_count": None,
                "hoisted_comment_id": None,
                "hoisted_reply_id": None
            }, separators=(',', ':'))
            
            encoded_vars = quote(variables)
            doc_id = GRAPHQL_DOC_IDS.get("reel" if shortcode.startswith("reel") else "post")
            
            payload = f"variables={encoded_vars}&doc_id={doc_id}"
            
            # Execute fetch via page.evaluate to use browser's networking (avoids CORS)
            result = await asyncio.wait_for(
                api_page.evaluate(f"""async () => {{
                    try {{
                        const response = await fetch("https://www.instagram.com/graphql/query", {{
                            method: "POST",
                            headers: {{
                                "Content-Type": "application/x-www-form-urlencoded",
                                "X-IG-App-ID": "{INSTAGRAM_HEADERS['x-ig-app-id']}",
                                "X-ASBD-ID": "{INSTAGRAM_HEADERS['x-asbd-id']}",
                                "X-Requested-With": "XMLHttpRequest",
                                "Referer": "https://www.instagram.com/p/{shortcode}/"
                            }},
                            body: "{payload}",
                            credentials: "include"
                        }});
                        
                        if (!response.ok) return null;
                        return await response.json();
                    }} catch (e) {{
                        return null;
                    }}
                }}"""),
                timeout=8.0
            )
            
            await api_page.close()
            
            if not result or not isinstance(result, dict):
                return None
            
            # Navigate to caption in response
            media = (
                result.get("data", {}).get("xdt_shortcode_media") or
                result.get("data", {}).get("shortcode_media")
            )
            
            if not media:
                return None
            
            # Try edge_media_to_caption first
            edges = media.get("edge_media_to_caption", {}).get("edges", [])
            if edges and edges[0].get("node", {}).get("text"):
                caption = edges[0]["node"]["text"]
                self.logger.debug(f"GraphQL caption: {len(caption)} chars", indent=2)
                return caption
            
            # Fallback to accessibility_caption
            accessibility = media.get("accessibility_caption")
            if accessibility and len(accessibility) > 10:
                return accessibility
                
            # Try caption object directly
            caption_obj = media.get("caption")
            if caption_obj and caption_obj.get("text"):
                return caption_obj["text"]
                
        except Exception as e:
            self.logger.debug(f"GraphQL error: {e}", indent=2)
            
        return None
    
    # ── Strategy 2: JSON-LD Structured Data ────────────────────────────
    
    @staticmethod
    def extract_jsonld(html: str) -> Optional[str]:
        """Extract from schema.org JSON-LD"""
        patterns = [
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            r'<script type="application/ld\+json">(.*?)</script>',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.DOTALL | re.I):
                try:
                    blob = json.loads(match.group(1))
                    if isinstance(blob, list):
                        blob = blob[0] if blob else {}
                    
                    # Try multiple caption fields
                    caption = (
                        blob.get("caption", {}).get("text") if isinstance(blob.get("caption"), dict) else
                        blob.get("caption") or
                        blob.get("description") or
                        blob.get("articleBody") or
                        ""
                    )
                    
                    if caption and len(caption) > 5:
                        return caption.strip()
                except Exception:
                    continue
        return None
    
    # ── Strategy 3: Meta Tags ────────────────────────────────────────
    
    @staticmethod
    def extract_meta(html: str) -> Optional[str]:
        """Extract from meta tags"""
        # og:description, twitter:description, description
        patterns = [
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{20,})["\']',
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,})["\']',
            r'<meta[^>]+property=["\']twitter:description["\'][^>]+content=["\']([^"\']{20,})["\']',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.I)
            if match:
                raw = match.group(1)
                # Unescape HTML entities
                raw = raw.replace("&quot;", '"').replace("&amp;", "&").replace("&#39;", "'")
                # Clean Instagram prefixes
                cleaned = re.sub(r'^[^:]+:\s*', "", raw).strip()
                cleaned = re.sub(r'^\d+\s+(?:Likes?|Comments?|Views?)[,\s]*', "", cleaned, flags=re.I)
                if len(cleaned) > 15:  # Higher threshold to avoid generic descriptions
                    return cleaned
        return None
    
    # ── Strategy 4: Hydration Data (__additionalDataLoaded) ──────────
    
    @staticmethod
    def extract_hydration(html: str) -> Optional[str]:
        """Extract from Instagram's hydration data"""
        patterns = [
            r'window\.__additionalDataLoaded\s*=\s*(\{.*?\});</script>',
            r'window\.__additionalData\s*=\s*(\{.*?\});</script>',
            r'window\._sharedData\s*=\s*(\{.*?\});</script>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if not match:
                continue
            
            try:
                data = json.loads(match.group(1))
                
                # Navigate various data structures
                media = None
                if "graphql" in str(data):
                    media = (
                        data.get("graphql", {}).get("shortcode_media") or
                        data.get("data", {}).get("xdt_shortcode_media") or
                        data.get("data", {}).get("shortcode_media")
                    )
                
                if media:
                    edges = media.get("edge_media_to_caption", {}).get("edges", [])
                    if edges and edges[0].get("node", {}).get("text"):
                        return edges[0]["node"]["text"]
                    
                    accessibility = media.get("accessibility_caption")
                    if accessibility and len(accessibility) > 10:
                        return accessibility
                        
            except Exception:
                continue
        
        return None
    
    # ── Strategy 5: Inline JSON Patterns ───────────────────────────────
    
    @staticmethod
    def extract_inline_json(html: str) -> Optional[str]:
        """Extract from inline JSON in scripts"""
        # Look for xdt_shortcode_media or shortcode_media patterns
        patterns = [
            r'"xdt_shortcode_media"\s*:\s*(\{.*?"edge_media_to_caption".*?\}(?=\s*,|\s*\}))',
            r'"shortcode_media"\s*:\s*(\{.*?"edge_media_to_caption".*?\}(?=\s*,|\s*\}))',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.DOTALL):
                try:
                    # Balance braces to extract valid JSON
                    json_str = match.group(1)
                    media = json.loads(json_str)
                    
                    edges = media.get("edge_media_to_caption", {}).get("edges", [])
                    if edges and edges[0].get("node", {}).get("text"):
                        return edges[0]["node"]["text"]
                        
                except Exception:
                    continue
        
        return None
    
    # ── Strategy 6: HTML Patterns (Last Resort) ──────────────────────
    
    @staticmethod
    def extract_html_patterns(html: str) -> Optional[str]:
        """Extract from raw HTML patterns"""
        # Look for caption divs or spans
        patterns = [
            r'<div[^>]*class="[^"]*caption[^"]*"[^>]*>(.*?)</div>',
            r'<span[^>]*class="[^"]*caption[^"]*"[^>]*>(.*?)</span>',
            r'data-testid="post-caption"[^>]*>(.*?)</div>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.I)
            if match:
                # Strip HTML tags
                text = re.sub(r'<[^>]+>', '', match.group(1))
                text = text.replace("&quot;", '"').replace("&amp;", "&")
                if len(text) > 10:
                    return text.strip()
        
        return None
    
    # ── Master Extraction Method ─────────────────────────────────────
    
    async def extract(self, html: Optional[bytes], shortcode: str, context: Optional[BrowserContext] = None) -> Optional[str]:
        """
        Try all strategies in priority order.
        Returns caption or None.
        """
        # Strategy 1: GraphQL API (if context available)
        if context:
            self.logger.debug("Trying GraphQL API...", indent=2)
            caption = await self.extract_graphql(shortcode, context)
            if caption:
                self.logger.debug("✓ GraphQL", indent=2)
                return caption
        
        if not html:
            return None
        
        try:
            html_str = html.decode("utf-8", errors="ignore")
        except Exception:
            return None
        
        # Strategy 2: JSON-LD
        self.logger.debug("Trying JSON-LD...", indent=2)
        caption = self.extract_jsonld(html_str)
        if caption:
            self.logger.debug("✓ JSON-LD", indent=2)
            return caption
        
        # Strategy 3: Meta tags
        self.logger.debug("Trying Meta tags...", indent=2)
        caption = self.extract_meta(html_str)
        if caption:
            self.logger.debug("✓ Meta tags", indent=2)
            return caption
        
        # Strategy 4: Hydration data
        self.logger.debug("Trying Hydration data...", indent=2)
        caption = self.extract_hydration(html_str)
        if caption:
            self.logger.debug("✓ Hydration", indent=2)
            return caption
        
        # Strategy 5: Inline JSON
        self.logger.debug("Trying Inline JSON...", indent=2)
        caption = self.extract_inline_json(html_str)
        if caption:
            self.logger.debug("✓ Inline JSON", indent=2)
            return caption
        
        # Strategy 6: HTML patterns
        self.logger.debug("Trying HTML patterns...", indent=2)
        caption = self.extract_html_patterns(html_str)
        if caption:
            self.logger.debug("✓ HTML patterns", indent=2)
            return caption
        
        return None


# ══════════════════════════════════════════════
#  BULLETPROOF NAVIGATION
# ══════════════════════════════════════════════

class BulletproofNavigator:
    """Cascading navigation that never hangs"""
    
    @staticmethod
    async def goto(page: Page, url: str, logger: DetailedLogger) -> Tuple[bool, Optional[str]]:
        for strategy, timeout in WAIT_STRATEGIES:
            logger.debug(f"Nav: {strategy} ({timeout}ms)", indent=2)
            
            try:
                response = await asyncio.wait_for(
                    page.goto(url, wait_until=strategy, timeout=timeout),
                    timeout=(timeout / 1000) + 2
                )
                
                if response is None:
                    continue
                
                status = response.status
                
                if status == 429:
                    return False, "Rate limited"
                if status >= 500:
                    continue
                if status < 400:
                    current_url = page.url
                    if any(x in current_url for x in ["challenge", "checkpoint", "accounts/login"]):
                        return False, "Blocked"
                    return True, None
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                if "timeout" in str(e).lower():
                    continue
                continue
        
        return False, "All navigation strategies failed"


# ══════════════════════════════════════════════
#  BULLETPROOF SCRAPER 2026
# ══════════════════════════════════════════════

class InstagramCaptionScraper2026:
    """
    2026 bulletproof scraper with reliable caption extraction.
    Uses GraphQL API first, falls back to HTML parsing.
    """
    
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        ]
        self.navigator = BulletproofNavigator()
        self.caption_extractor = CaptionExtractor2026(logger)
    
    async def _human_delay(self, min_ms: int = 600, max_ms: int = 1400):
        await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)
    
    async def dismiss_popups(self, page: Page):
        selectors = [
            'button:has-text("Not now")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept")',
        ]
        
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.is_visible(timeout=500):
                    await locator.click(timeout=800)
                    await asyncio.sleep(0.2)
            except:
                pass
    
    async def _capture_html(self, page: Page) -> Optional[bytes]:
        """Capture HTML safely"""
        try:
            html = await asyncio.wait_for(
                page.evaluate("document.documentElement.outerHTML"),
                timeout=5.0
            )
            if html and len(html) > 500:
                return html.encode('utf-8')
        except Exception:
            pass
        return None
    
    async def _wait_for_content(self, page: Page) -> bool:
        """Wait for content indicators"""
        selectors = ['article', 'header', '[role="main"]', 'img', 'h2']
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=2000)
                return True
            except:
                continue
        return False
    
    async def scrape_single_post(
        self,
        context: BrowserContext,
        post_url: str,
        shortcode: str,
        post_index: int,
    ) -> ScrapingResult:
        """Scrape with all 2026 strategies"""
        t0 = time.monotonic()
        post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"
        
        self.logger.info(f"[{post_index:>2}] {post_type} {shortcode}", indent=1)
        
        page = None
        
        try:
            # Create fresh page
            page = await context.new_page()
            await page.route("**/*", smart_route_handler)
            
            # Anti-detection
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            """)
            
            # Navigate
            self.logger.debug(f"nav →", indent=2)
            success, error = await self.navigator.goto(page, post_url, self.logger)
            
            if not success:
                return ScrapingResult(success=False, error=f"Nav failed: {error}")
            
            if any(x in page.url for x in ["challenge", "checkpoint", "accounts/login"]):
                return ScrapingResult(success=False, error="Blocked")
            
            await self.dismiss_popups(page)
            await self._wait_for_content(page)
            await asyncio.sleep(0.5)  # Let JS hydrate
            
            # Extract HTML
            html_bytes = await self._capture_html(page)
            
            # Use 2026 extractor with GraphQL + HTML fallbacks
            caption = await self.caption_extractor.extract(
                html_bytes, 
                shortcode, 
                context  # Enables GraphQL strategy
            )
            
            elapsed = time.monotonic() - t0
            
            if caption:
                self.logger.success(
                    f"[{post_index:>2}] ✓ {shortcode:<15} {len(caption):>3} chars {elapsed:>4.1f}s",
                    indent=1,
                )
                return ScrapingResult(
                    success=True,
                    data={"url": post_url, "shortcode": shortcode, "caption": caption.strip()},
                )
            
            self.logger.warning(
                f"[{post_index:>2}] no caption {shortcode} {elapsed:.1f}s",
                indent=1,
            )
            return ScrapingResult(
                success=True,
                data={"url": post_url, "shortcode": shortcode, "caption": ""}
            )
            
        except Exception as e:
            elapsed = time.monotonic() - t0
            self.logger.error(f"[{post_index:>2}] {type(e).__name__} {elapsed:.1f}s", indent=1)
            return ScrapingResult(success=False, error=str(e)[:80])
            
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
    
    async def _collect_post_urls(self, page: Page, post_limit: int) -> List[str]:
        """Collect post URLs"""
        post_urls: List[str] = []
        last_height: int = 0
        stale_rounds: int = 0
        MAX_STALE = 3
        MAX_SCROLLS = 15
        
        js_collect = r"""
            () => {
                const links = Array.from(
                    document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]')
                );
                return [...new Set(links.map(a => {
                    try {
                        return new URL(a.href).pathname;
                    } catch {
                        return a.href.replace(/^https:\/\/[^\/]+/, '');
                    }
                }))];
            }
        """
        
        for i in range(MAX_SCROLLS):
            links = await asyncio.wait_for(page.evaluate(js_collect), timeout=5.0) or []
            
            full_links = [
                f"https://www.instagram.com{link}" if not link.startswith('http') else link
                for link in links
            ]
            
            new = [u for u in full_links if u not in post_urls]
            post_urls.extend(new)
            
            if new:
                self.logger.info(f"Scroll {i+1:>2}  +{len(new)} → {len(post_urls)} total", indent=2)
                stale_rounds = 0
            else:
                stale_rounds += 1
            
            if len(post_urls) >= post_limit:
                break
            
            if stale_rounds >= MAX_STALE:
                break
            
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.8)
                
                new_height = await asyncio.wait_for(
                    page.evaluate("document.body.scrollHeight"),
                    timeout=3.0
                ) or last_height
                
                if new_height == last_height:
                    stale_rounds += 1
                else:
                    last_height = new_height
                    stale_rounds = 0
            except Exception:
                break
        
        return post_urls[:post_limit]
    
    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        """Main workflow"""
        t_total = time.monotonic()
        
        self.logger.phase(
            "Instagram Scraper 2026",
            f"@{username}  ·  limit {post_limit}  ·  RELIABLE MODE",
        )
        
        browser = None
        context = None
        
        try:
            async with async_playwright() as p:
                self.logger.section("Browser")
                self.logger.info("Launching Chromium …", indent=2)
                
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--window-size=1280,720",
                    ],
                )
                self.logger.success("Browser ready", indent=2)
                self.logger.section_end()
                
                context = await browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                    timezone_id="America/New_York",
                    extra_http_headers=INSTAGRAM_HEADERS,
                )
                await context.add_cookies(self.cookies)
                self.logger.debug(f"Cookies: {len(self.cookies)}", indent=2)
                
                # Load profile
                self.logger.phase("Load Profile", f"@{username}")
                self.logger.section("Navigation")
                
                profile_page = await context.new_page()
                await profile_page.route("**/*", smart_route_handler)
                
                profile_url = f"https://www.instagram.com/{username}/"
                success, error = await self.navigator.goto(profile_page, profile_url, self.logger)
                
                if not success:
                    self.logger.error(f"Profile load failed: {error}", indent=1)
                    return []
                
                if any(x in profile_page.url for x in ["challenge", "checkpoint", "accounts/login"]):
                    self.logger.error("Access blocked", indent=1)
                    return []
                
                self.logger.success("Profile loaded", indent=2)
                self.logger.section_end()
                
                await self.dismiss_popups(profile_page)
                await asyncio.sleep(1.0)
                
                # Discover posts
                self.logger.phase("Discover Posts", f"Target: {post_limit}")
                self.logger.section("Grid scroll")
                post_urls = await self._collect_post_urls(profile_page, post_limit)
                await profile_page.close()
                self.logger.section_end(f"{len(post_urls)} URLs")
                
                if not post_urls:
                    self.logger.error("No posts found", indent=1)
                    return []
                
                # Scrape posts
                self.logger.phase("Scrape Captions", f"{len(post_urls)} posts")
                self.logger.section("Sequential scraping")
                
                posts: List[Dict] = []
                failures: List[str] = []
                
                for i, url in enumerate(post_urls, 1):
                    sc = url.split("/p/")[-1].split("/reel/")[-1].split("/tv/")[-1].split("/")[0].split("?")[0]
                    
                    result = await self.scrape_single_post(context, url, sc, i)
                    
                    if result.success:
                        posts.append(result.data)
                    else:
                        failures.append(f"post {i}: {result.error}")
                    
                    if i < len(post_urls):
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                    
                    self.logger.progress(i, len(post_urls), f"{len(posts)} ok")
                
                self.logger.section_end()
                
                # Summary
                elapsed_total = time.monotonic() - t_total
                self.logger.phase("Summary", f"Total: {elapsed_total:.1f}s")
                self.logger.separator()
                self.logger.success(f"Scraped:  {len(posts)}/{len(post_urls)}", indent=1)
                
                if failures:
                    self.logger.warning(f"Failed:   {len(failures)}/{len(post_urls)}", indent=1)
                
                captioned = sum(1 for p in posts if p.get("caption"))
                self.logger.info(f"Captions: {captioned}/{len(posts)}", indent=1)
                
                if captioned > 0 and posts:
                    lengths = [len(p["caption"]) for p in posts if p.get("caption")]
                    self.logger.info(f"Avg:      {sum(lengths)//len(lengths)} chars", indent=1)
                    self.logger.info(f"Speed:    {elapsed_total/len(post_urls):.1f}s/post", indent=1)
                
                self.logger.separator()
                
                return posts
                
        except Exception as e:
            import traceback
            self.logger.error(f"Fatal: {type(e).__name__}: {str(e)[:80]}", indent=1)
            self.logger.debug(traceback.format_exc(), indent=1)
            return []
            
        finally:
            self.logger.section("Cleanup")
            try:
                if context:
                    await context.close()
            except:
                pass
            try:
                if browser:
                    await browser.close()
                    self.logger.success("Browser closed", indent=2)
            except:
                pass
            self.logger.section_end()


# ══════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════

async def fetch_ig_urls(
    account: str,
    cookies: List[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """2026 bulletproof caption scraper"""
    account = account.lstrip("@")
    
    logger.phase("fetch_ig_urls 2026", f"@{account}")
    logger.section("Cookie setup")
    
    if cookies is None:
        logger.info("Reading IG_COOKIES env var", indent=2)
        raw = os.getenv("IG_COOKIES", "")
        if not raw:
            logger.error("IG_COOKIES not set", indent=2)
            return []
        try:
            cookies = json.loads(raw)
            logger.success(f"Loaded {len(cookies)} cookies", indent=2)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}", indent=2)
            return []
    else:
        logger.success(f"Using {len(cookies)} cookies", indent=2)
    
    session_ok = any(c.get("name") == "sessionid" for c in cookies)
    logger.info(f"session: {session_ok}", indent=2)
    if not session_ok:
        logger.warning("sessionid missing - may affect reliability", indent=2)
    
    logger.section_end()
    
    scraper = InstagramCaptionScraper2026(cookies=cookies, logger=logger)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )