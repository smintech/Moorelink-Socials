import asyncio
import json
import random
import re
import time
import signal
from typing import Dict, Optional, Any, Tuple, Callable, List
from urllib.parse import quote, urlencode
import os
from dataclasses import dataclass
from contextlib import asynccontextmanager

from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
    Response,
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
#  2026 SPEED-OPTIMIZED CONFIGURATION
# ══════════════════════════════════════════════

NAVIGATION_TIMEOUT = 20_000      # 20s max for navigation
REEL_NAV_TIMEOUT = 12_000        # Reels: fast, don't wait for video
POST_NAV_TIMEOUT = 25_000        # Posts: reliable loading

DOM_WAIT_REEL = 3_000            # Reels: minimal DOM wait
DOM_WAIT_POST = 8_000            # Posts: full structure wait
HTML_CAPTURE_TIMEOUT = 8.0       # Fast capture
PER_POST_TIMEOUT = 35.0          # Hard ceiling per post

WAIT_STRATEGY_REEL = "domcontentloaded"   # Fast for reels
WAIT_STRATEGY_POST = "commit"             # Reliable for posts

CDN_ALLOWLIST = ("cdninstagram", "fbcdn")
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

_BLOCK_TYPES = frozenset({"font", "stylesheet", "image", "media"})

# Reels: Block everything including CDN media
REEL_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net", "fbcdn.net",
    "cdninstagram.com", "blob:", "video", "mp4", "webm", 
    "instagram.com/api/", "scorecardresearch", "omtrdc.net",
    "googletagmanager"
)

# Posts: Allow CDN for OG image tags
POST_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net", "googletagmanager"
)

INSTAGRAM_HEADERS = {
    "x-ig-app-id": "936619743392459",
    "x-asbd-id": "129477",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
}

PROFILE_POSTS_DOC_ID = "9310670392322965"
USER_INFO_DOC_ID = "2398832706970914"


# ══════════════════════════════════════════════
#  DETAILED LOGGER (unchanged)
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
#  RESULT DATACLASS
# ══════════════════════════════════════════════

@dataclass
class ScrapingResult:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None


# ══════════════════════════════════════════════
#  POST TYPE DETECTION
# ══════════════════════════════════════════════

def detect_post_type(url: str) -> str:
    """Detect if URL is Reel, TV, or standard Post"""
    url_lower = url.lower()
    if "/reel/" in url_lower:
        return "REEL"
    elif "/tv/" in url_lower:
        return "TV"
    elif "/p/" in url_lower:
        return "POST"
    else:
        return "UNKNOWN"


# ══════════════════════════════════════════════
#  STRATEGIC ROUTE HANDLERS
# ══════════════════════════════════════════════

async def reel_route_handler(route):
    """Aggressive blocking for Reels - block all media/CDN"""
    try:
        url = route.request.url
        rtype = route.request.resource_type
        
        if rtype in _BLOCK_TYPES:
            await route.abort()
            return
        if any(d in url for d in REEL_BLOCK_DOMAINS):
            await route.abort()
            return
        await route.continue_()
    except Exception:
        await route.abort()

async def post_route_handler(route):
    """Standard blocking for Posts - allow CDN images"""
    try:
        url = route.request.url
        rtype = route.request.resource_type
        
        if rtype in _BLOCK_TYPES:
            await route.abort()
            return
        if any(d in url for d in POST_BLOCK_DOMAINS):
            await route.abort()
            return
        await route.continue_()
    except Exception:
        await route.abort()


# ══════════════════════════════════════════════
#  CAPTION PARSER
# ══════════════════════════════════════════════

class InstagramCaptionParser:
    @classmethod
    def _unescape(cls, s: str) -> str:
        try:
            return json.loads(f'"{s}"')
        except Exception:
            return s.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
    
    @classmethod
    def _clean_og_description(cls, raw: str) -> str:
        text = cls._unescape(raw).strip()
        text = re.sub(r'\s*on Instagram.*$', '', text, flags=re.I)
        text = re.sub(r'\s*\(.*?\)\s*on Instagram.*$', '', text, flags=re.I)
        text = re.sub(r'\s*View all \d+ comments?.*$', '', text, flags=re.I)
        text = re.sub(r'\s*·\s*View all.*$', '', text, flags=re.I)
        text = re.sub(r'\s*•\s*.*$', '', text, flags=re.I)
        text = re.sub(r'\s*\d{1,3}(,\d{3})*(\.\d+)?\s*(likes?|views?|comments?).*$', '', text, flags=re.I)
        if ':' in text[:100]:
            text = text.split(':', 1)[1].strip()
        text = re.sub(r'^@?[\w._]+\s*', '', text, flags=re.I)
        return text.strip()
    
    @classmethod
    def parse(cls, html: bytes, shortcode: str) -> Optional[str]:
        if not html or len(html) < 1000:
            return None
        
        try:
            text = html.decode("utf-8", errors="ignore")
        except Exception:
            return None
        
        # JSON-LD
        jsonld_pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.I,
        )
        
        for match in jsonld_pattern.finditer(text):
            try:
                blob = json.loads(match.group(1))
                if isinstance(blob, list):
                    blob = blob[0] if blob else {}
                
                caption = (
                    blob.get("caption") or
                    blob.get("description") or
                    blob.get("articleBody") or
                    blob.get("text") or
                    ""
                )
                
                if caption and len(caption) > 8:
                    return caption.strip()
            except Exception:
                pass
        
        # OG Description
        og_desc_pattern = re.compile(
            r'<meta[^>]+(?:property=["\']og:description["\']|name=["\']description["\'])'
            r'[^>]+content=["\']([^"\']{10,})["\']',
            re.I
        )
        
        match = og_desc_pattern.search(text)
        if match:
            cleaned = cls._clean_og_description(match.group(1))
            if len(cleaned) > 10:
                return cleaned
        
        # Fallback patterns
        patterns = [
            r'"edge_media_to_caption"\s*:\s*\{[^}]*"edges"\s*:\s*\[\s*\{[^}]*"node"\s*:\s*'
            r'\{[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
            r'"caption"\s*:\s*"((?:[^"\\]|\\.){10,})"',
            r'"caption_text"\s*:\s*"((?:[^"\\]|\\.){10,})"',
            r'\{"text"\s*:\s*"((?:[^"\\]|\\.){10,})"\}',
            r'"caption":\s*"((?:[^"\\]|\\.)+?)"\s*,',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                caption = cls._unescape(match.group(1))
                if len(caption) > 10 and not re.match(r'^[\w_]+$', caption):
                    return caption.strip()
        
        return None


# ══════════════════════════════════════════════
#  SAFE PAGE CONTEXT MANAGER 
# ══════════════════════════════════════════════

@asynccontextmanager
async def managed_page(context: BrowserContext, post_type: str = "POST"):
    """
    Guaranteed page cleanup - closes even if exceptions occur
    Playwright-compatible close() with no unsupported arguments
    """
    page = None
    try:
        page = await context.new_page()
        
        # Apply appropriate route handler
        if post_type == "REEL":
            await page.route("**/*", reel_route_handler)
        else:
            await page.route("**/*", post_route_handler)
        
        yield page
        
    finally:
        if page:
            try:
                # Use asyncio.wait_for to prevent hanging
                await asyncio.wait_for(page.close(), timeout=5.0)
                logger.debug(f"Page closed ({post_type})", indent=2)
            except asyncio.TimeoutError:
                logger.debug(f"Page close timeout ({post_type})", indent=2)
                # Force context closure will clean up hung pages
            except Exception as e:
                logger.debug(f"Page close error ({post_type}): {e}", indent=2)


# ══════════════════════════════════════════════
#  MAIN SCRAPER CLASS
# ══════════════════════════════════════════════

class InstagramCaptionScraper2026:
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        ]
    
    async def _human_delay(self, min_ms: int = 800, max_ms: int = 2200):
        await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)
    
    async def dismiss_popups(self, page: Page):
        selectors = [
            'button:has-text("Not Now")',
            'button:has-text("Not now")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Allow essential and optional cookies")',
            'button:has-text("Accept")',
            'button:has-text("Accept all")',
            '[aria-label="Close"]',
        ]
        
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.is_visible(timeout=500):
                    await locator.click(force=True, timeout=1000)
                    await asyncio.sleep(0.2)
            except:
                pass
    
    async def _capture_html_fast(self, page: Page) -> bytes:
        """Fast HTML capture - no waiting"""
        try:
            html = await asyncio.wait_for(
                page.content(),
                timeout=HTML_CAPTURE_TIMEOUT
            )
            if len(html) > 2000:
                return html.encode('utf-8')
        except Exception:
            pass
        
        # Fallback
        try:
            html = await page.evaluate("document.documentElement.outerHTML")
            return html.encode('utf-8')
        except Exception:
            return b""
    
    async def strategic_goto(self, page: Page, url: str, post_type: str) -> Tuple[bool, Optional[Response]]:
        """Navigate with post-type specific strategy"""
        try:
            if post_type == "REEL":
                # REEL: domcontentloaded - fast, ignore video assets
                response = await page.goto(
                    url, 
                    wait_until=WAIT_STRATEGY_REEL,
                    timeout=REEL_NAV_TIMEOUT
                )
            else:
                # POST: commit - reliable, then we control DOM
                response = await page.goto(
                    url,
                    wait_until=WAIT_STRATEGY_POST,
                    timeout=POST_NAV_TIMEOUT
                )
            
            if not response or response.status >= 400:
                return False, response
            
            current_url = page.url
            if any(x in current_url for x in ["challenge", "checkpoint", "accounts/login"]):
                return False, response
                
            return True, response
            
        except Exception as e:
            self.logger.debug(f"Nav error ({post_type}): {type(e).__name__}", indent=2)
            return False, None
    
    async def strategic_content_wait(self, page: Page, post_type: str):
        """Wait for content based on post type"""
        if post_type == "REEL":
            # Reels: Meta tags in initial HTML, minimal wait
            try:
                await page.wait_for_selector("body", timeout=DOM_WAIT_REEL)
            except:
                pass
            await asyncio.sleep(0.3)  # Tiny buffer for JS
            
        else:
            # Posts: Wait for article structure
            selectors = ['article', 'main', 'header']
            for sel in selectors:
                try:
                    await page.wait_for_selector(sel, timeout=DOM_WAIT_POST // len(selectors))
                    return
                except:
                    continue
            await asyncio.sleep(0.5)
    
    async def scrape_single_post(self, url: str, shortcode: str, index: int, context: BrowserContext) -> ScrapingResult:
        """
        Scrape single post with guaranteed page cleanup
        """
        t0 = time.monotonic()
        post_type = detect_post_type(url)
        
        self.logger.info(f"[{index:>2}] {post_type} {shortcode}", indent=1)
        
        # Use context manager for guaranteed cleanup
        async with managed_page(context, post_type) as page:
            # Navigate
            success, _ = await self.strategic_goto(page, url, post_type)
            if not success:
                return ScrapingResult(success=False, error="Nav failed")
            
            # Dismiss popups
            await self.dismiss_popups(page)
            
            # Wait for content
            await self.strategic_content_wait(page, post_type)
            
            # Capture HTML
            html_bytes = await self._capture_html_fast(page)
            
            # Parse
            caption = InstagramCaptionParser.parse(html_bytes, shortcode) if len(html_bytes) > 1000 else None
            
            elapsed = time.monotonic() - t0
            
            if caption:
                self.logger.success(f"✓ {shortcode} {len(caption)} chars {elapsed:.1f}s", indent=1)
                return ScrapingResult(success=True, data={
                    "url": url, 
                    "shortcode": shortcode, 
                    "caption": caption.strip(),
                    "type": post_type
                })
            else:
                self.logger.warning(f"✗ {shortcode} no caption {elapsed:.1f}s", indent=1)
                return ScrapingResult(success=True, data={
                    "url": url, 
                    "shortcode": shortcode, 
                    "caption": "",
                    "type": post_type
                })
    
    async def scrape_posts_parallel(self, context: BrowserContext, post_urls: List[str], max_concurrent: int = 2) -> List[Dict]:
        """
        Parallel scraping with semaphore and guaranteed cleanup per task
        """
        posts: List[Dict] = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def scrape_with_semaphore(url: str, index: int) -> Optional[Dict]:
            async with semaphore:
                post_type = detect_post_type(url)
                shortcode = url.split('/')[-2]
                
                # Type-specific timeout
                timeout = REEL_NAV_TIMEOUT/1000 + 8 if post_type == "REEL" else POST_NAV_TIMEOUT/1000 + 15
                
                try:
                    result = await asyncio.wait_for(
                        self.scrape_single_post(url, shortcode, index, context),
                        timeout=timeout
                    )
                    return result.data if result.success else None
                except asyncio.TimeoutError:
                    self.logger.error(f"✗ {shortcode} HARD TIMEOUT", indent=1)
                    return None
        
        # Create tasks
        tasks = [scrape_with_semaphore(url, i+1) for i, url in enumerate(post_urls)]
        
        # Process with progress
        completed = 0
        for coro in asyncio.as_completed(tasks):
            completed += 1
            result = await coro
            if result:
                posts.append(result)
            self.logger.progress(len(posts), len(post_urls), f"#{completed}")
        
        return posts
    
    async def _collect_post_urls(self, context: BrowserContext, username: str, post_limit: int, shutdown_requested: Callable[[], bool]) -> List[str]:
        """Collect post URLs from profile with managed page"""
        profile_url = f"https://www.instagram.com/{username}/"
        post_urls: List[str] = []
        
        # Use POST strategy for profile (more reliable)
        async with managed_page(context, "POST") as page:
            # Navigate to profile
            try:
                response = await page.goto(
                    profile_url,
                    wait_until="domcontentloaded",
                    timeout=NAVIGATION_TIMEOUT
                )
                
                if not response or response.status >= 400:
                    self.logger.error("Profile nav failed", indent=1)
                    return []
                
                if any(x in page.url for x in ["challenge", "checkpoint", "login"]):
                    self.logger.error("Access blocked", indent=1)
                    return []
                
            except Exception as e:
                self.logger.error(f"Profile nav error: {e}", indent=1)
                return []
            
            await self.dismiss_popups(page)
            await asyncio.sleep(1.0)
            
            # Scroll to collect URLs
            self.logger.section("Collect URLs")
            
            last_height = 0
            stale_rounds = 0
            MAX_STALE = 3
            MAX_SCROLLS = 15
            
            js_collect = """
            () => {
                const links = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]'));
                return [...new Set(links.map(a => a.href.split('?')[0]))];
            }
            """
            
            for i in range(MAX_SCROLLS):
                if shutdown_requested():
                    self.logger.warning("Shutdown requested, stopping scroll", indent=1)
                    break
                
                links = await page.evaluate(js_collect) or []
                new = [u for u in links if u not in post_urls]
                post_urls.extend(new)
                
                if new:
                    self.logger.info(f"Scroll {i+1} +{len(new)} → {len(post_urls)}", indent=2)
                    stale_rounds = 0
                else:
                    stale_rounds += 1
                
                if len(post_urls) >= post_limit or stale_rounds >= MAX_STALE:
                    break
                
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(1.0, 2.0))
                
                new_height = await page.evaluate("document.body.scrollHeight") or last_height
                if new_height == last_height:
                    stale_rounds += 1
                else:
                    last_height = new_height
                    stale_rounds = 0
            
            self.logger.section_end(f"{len(post_urls)} found")
        
        return post_urls[:post_limit]
    
    async def scrape_profile_api(self, context: BrowserContext, username: str, post_limit: int) -> List[Dict]:
        """API method with better error handling"""
        posts: List[Dict] = []
        
        try:
            profile_url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
            self.logger.debug(f"API profile: {profile_url}", indent=2)
            
            response = await context.request.get(
                profile_url,
                headers=INSTAGRAM_HEADERS,
                timeout=15000
            )
            
            if response.status == 429:
                self.logger.warning("API 429 - rate limited", indent=2)
                return []  # Force fallback to HTML
                
            if not response.ok:
                self.logger.warning(f"API {response.status}", indent=2)
                raise ValueError(f"Status {response.status}")
            
            data = await response.json()
            user = data.get("data", {}).get("user")
            if not user:
                raise ValueError("No user data")
            
            user_id = user["id"]
            timeline = user.get("edge_owner_to_timeline_media", {})
            posts.extend(self._extract_posts(timeline.get("edges", [])))
            
            page_info = timeline.get("page_info", {})
            
            while page_info.get("has_next_page") and len(posts) < post_limit:
                variables = {
                    "id": user_id,
                    "first": min(50, post_limit - len(posts)),
                    "after": page_info["end_cursor"]
                }
                
                params = {
                    "variables": json.dumps(variables, separators=(',', ':')),
                    "doc_id": PROFILE_POSTS_DOC_ID
                }
                
                self.logger.debug(f"Paginate after={page_info['end_cursor'][:20]}...", indent=2)
                
                pag_response = await context.request.get(
                    "https://www.instagram.com/graphql/query",
                    params=params,
                    headers=INSTAGRAM_HEADERS,
                    timeout=15000
                )
                
                if pag_response.status == 429:
                    self.logger.warning("API 429 on pagination", indent=2)
                    break
                    
                if not pag_response.ok:
                    raise ValueError(f"Pag {pag_response.status}")
                
                pag_data = await pag_response.json()
                pag_timeline = pag_data["data"]["user"]["edge_owner_to_timeline_media"]
                
                posts.extend(self._extract_posts(pag_timeline["edges"]))
                page_info = pag_timeline["page_info"]
                
                await self._human_delay(800, 1500)
            
            return posts[:post_limit]
            
        except Exception as e:
            self.logger.error(f"API error: {str(e)[:80]}", indent=1)
            return []
    
    def _extract_posts(self, edges: List[Dict]) -> List[Dict]:
        extracted = []
        for edge in edges:
            node = edge["node"]
            shortcode = node["shortcode"]
            typename = node.get("__typename", "")
            
            # Determine URL type
            if typename == "GraphVideo" and node.get("is_video"):
                url = f"https://www.instagram.com/reel/{shortcode}/"
                post_type = "REEL"
            else:
                url = f"https://www.instagram.com/p/{shortcode}/"
                post_type = "POST"
                
            caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
            caption = caption_edges[0]["node"]["text"] if caption_edges else ""
            
            extracted.append({
                "url": url,
                "shortcode": shortcode,
                "caption": caption.strip(),
                "type": post_type
            })
        return extracted
    
    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        """Main entry point with full resource management"""
        t_total = time.monotonic()
        
        self.logger.phase("IG Scraper 2026", f"@{username} limit {post_limit} API+HTML")
        
        browser = None
        context = None
        shutdown_requested = False
        
        def handle_sigterm():
            nonlocal shutdown_requested
            shutdown_requested = True
            self.logger.info("SIGTERM received - finishing current operations")
        
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
            loop.add_signal_handler(signal.SIGINT, handle_sigterm)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler
        
        try:
            async with async_playwright() as p:
                self.logger.section("Launch browser")
                
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--window-size=1280,720",
                        "--disable-extensions",
                        "--disable-background-networking",
                    ],
                )
                self.logger.success("Ready", indent=2)
                
                context = await browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                    timezone_id="America/New_York",
                    extra_http_headers=INSTAGRAM_HEADERS,
                )
                await context.add_cookies(self.cookies)
                self.logger.debug(f"Cookies {len(self.cookies)}", indent=2)
                
                # Try API first
                self.logger.phase("API attempt")
                posts = await self.scrape_profile_api(context, username, post_limit)
                
                if len(posts) >= post_limit:
                    self.logger.success(f"Full {len(posts)} via API", indent=1)
                elif posts:
                    self.logger.info(f"Partial {len(posts)} via API", indent=1)
                else:
                    self.logger.warning("API failed, fallback HTML", indent=1)
                    
                    # HTML Fallback
                    self.logger.phase("HTML Fallback")
                    
                    # Collect URLs
                    post_urls = await self._collect_post_urls(
                        context, username, post_limit, lambda: shutdown_requested
                    )
                    
                    if post_urls and not shutdown_requested:
                        # Scrape parallel
                        self.logger.section("Scrape posts")
                        posts = await self.scrape_posts_parallel(
                            context, post_urls[:post_limit]
                        )
                        self.logger.section_end(f"{len(posts)} ok")
                
                # Summary
                elapsed_total = time.monotonic() - t_total
                self.logger.phase("Summary", f"{elapsed_total:.1f}s")
                self.logger.separator()
                self.logger.success(f"Scraped {len(posts)}", indent=1)
                
                captioned = sum(1 for p in posts if p.get("caption"))
                self.logger.info(f"Captions {captioned}/{len(posts)}", indent=1)
                
                if captioned:
                    lengths = [len(p["caption"]) for p in posts if p.get("caption")]
                    self.logger.info(f"Avg {sum(lengths)//len(lengths)} chars", indent=1)
                
                if posts:
                    self.logger.info(f"Speed {elapsed_total/len(posts):.1f}s/post", indent=1)
                    
                    # Breakdown by type
                    reels = sum(1 for p in posts if p.get("type") == "REEL")
                    standard = len(posts) - reels
                    self.logger.info(f"Reels: {reels}, Posts: {standard}", indent=1)
                
                self.logger.separator()
                
                return posts
                
        except Exception as e:
            import traceback
            self.logger.error(f"Fatal {type(e).__name__}: {str(e)[:80]}", indent=1)
            self.logger.debug(traceback.format_exc(), indent=1)
            return []
            
        finally:
            # GUARANTEED CLEANUP - FIXED
            self.logger.section("Cleanup")
            
            if context:
                try:
                    # Close all pages first with timeout
                    pages = context.pages
                    for page in pages:
                        try:
                            await asyncio.wait_for(page.close(), timeout=3.0)
                        except:
                            pass  # Ignore individual page close errors
                    
                    await asyncio.wait_for(context.close(), timeout=5.0)
                    self.logger.success("Context closed", indent=2)
                except Exception as e:
                    self.logger.debug(f"Context cleanup error: {e}", indent=2)
            
            if browser:
                try:
                    await asyncio.wait_for(browser.close(), timeout=5.0)
                    self.logger.success("Browser closed", indent=2)
                except Exception as e:
                    self.logger.debug(f"Browser cleanup error: {e}", indent=2)


# ══════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════

async def fetch_ig_urls(
    account: str,
    cookies: List[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    account = account.lstrip("@")
    
    logger.phase("fetch_ig_urls", f"@{account}")
    logger.section("Cookies")
    
    if cookies is None:
        raw = os.getenv("IG_COOKIES", "")
        if not raw:
            logger.error("IG_COOKIES missing", indent=2)
            return []
        try:
            cookies = json.loads(raw)
            logger.success(f"Loaded {len(cookies)}", indent=2)
        except json.JSONDecodeError as e:
            logger.error(f"JSON error: {e}", indent=2)
            return []
    else:
        logger.success(f"Provided {len(cookies)}", indent=2)
    
    session_ok = any(c.get("name") == "sessionid" for c in cookies)
    logger.info(f"Session: {session_ok}", indent=2)
    if not session_ok:
        logger.warning("No sessionid - limits may apply", indent=2)
    
    logger.section_end()
    
    scraper = InstagramCaptionScraper2026(cookies=cookies, logger=logger)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )
