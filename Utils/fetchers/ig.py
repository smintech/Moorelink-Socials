import json
import random
import re
import time
import signal
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
#  BALANCED TIMEOUTS - Get Results, Avoid Hangs
# ══════════════════════════════════════════════

# Smart timeouts: enough time to get results, but prevent infinite hangs
NAVIGATION_TIMEOUT = 20_000      # 20 seconds (reasonable for Instagram)
NAVIGATION_HARD_LIMIT = 25.0     # 25 second absolute max

# HTML capture with progressive timeout reduction
HTML_CAPTURE_ATTEMPTS = [
    ("evaluate", 8.0),    # First try: 8 seconds (usually works)
    ("content", 6.0),     # Second try: 6 seconds
    ("evaluate", 4.0),    # Third try: 4 seconds (faster retry)
]

# Per-post budget: generous but bounded
PER_POST_TIMEOUT = 40.0          # 40 seconds per post (down from 400+)
POST_NAV_SETTLE_TIME = 1.5       # 1.5 seconds to settle

# Content detection
CONTENT_WAIT_TIMEOUT = 5.0       # 3 seconds to find content
NETWORKIDLE_TIMEOUT = 4000       # 4 seconds for network idle

# Other
POPUP_TIMEOUT = 2.0              # 2 seconds for popups
SCROLL_DELAY = 0.7               # Scroll delay

WAIT_STRATEGY = "commit"

CDN_ALLOWLIST = ("cdninstagram", "fbcdn")
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")

_BLOCK_TYPES = frozenset({"font", "stylesheet", "image", "media"})
_BLOCK_DOMAINS = (
    "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
    "scorecardresearch", "omtrdc.net", "googletagmanager",
)

INSTAGRAM_HEADERS = {
    "x-ig-app-id": "936619743392459",
    "x-asbd-id": "129477",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


# ══════════════════════════════════════════════
#  Shutdown Handler
# ══════════════════════════════════════════════

class ShutdownManager:
    """Global shutdown coordinator"""
    def __init__(self):
        self.shutdown_requested = False
        
    def request_shutdown(self):
        print("\n🛑 Shutdown requested - will finish current post and exit gracefully")
        self.shutdown_requested = True
        
    def is_shutting_down(self):
        return self.shutdown_requested


shutdown_manager = ShutdownManager()


def setup_signal_handlers():
    """Setup graceful shutdown handlers"""
    def signal_handler(signum, frame):
        shutdown_manager.request_shutdown()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


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
#  ENHANCED CAPTION PARSER
# ══════════════════════════════════════════════

class InstagramCaptionParser:
    """Enhanced parser with multiple strategies"""
    
    @classmethod
    def _unescape(cls, s: str) -> str:
        if not s:
            return ""
        try:
            return json.loads(f'"{s}"')
        except Exception:
            s = s.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
            s = s.replace("&quot;", '"').replace("&amp;", "&")
            s = s.replace("&lt;", "<").replace("&gt;", ">")
            return s
    
    @classmethod
    def _clean_caption(cls, raw: str) -> str:
        if not raw:
            return ""
        cleaned = cls._unescape(raw)
        cleaned = re.sub(r'^[^:]+:\s*"?', "", cleaned)
        cleaned = re.sub(r'^\d+\s+(?:Likes?|Comments?|Views?)[,\s]*', "", cleaned, flags=re.I)
        cleaned = cleaned.strip()
        if len(cleaned) < 5 or re.match(r'^[\w_.]+$', cleaned):
            return ""
        return cleaned
    
    @classmethod
    def parse(cls, html: bytes, shortcode: str) -> Optional[str]:
        """Parse with multiple strategies"""
        if not html or len(html) < 100:
            return None
        
        try:
            text = html.decode("utf-8", errors="ignore")
        except:
            return None
        
        if "instagram" not in text.lower():
            return None
        
        # Strategy 1: JSON-LD (very reliable)
        jsonld_pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.I,
        )
        for match in jsonld_pattern.finditer(text):
            try:
                blob = json.loads(match.group(1))
                if isinstance(blob, list):
                    blob = blob[0] if blob else {}
                for field in ["caption", "description", "articleBody"]:
                    caption = blob.get(field, "")
                    if caption:
                        cleaned = cls._clean_caption(caption)
                        if len(cleaned) > 5:
                            return cleaned
            except:
                pass
        
        # Strategy 2: Meta tags
        patterns = [
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{10,})["\']',
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{10,})["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                cleaned = cls._clean_caption(match.group(1))
                if len(cleaned) > 10:
                    return cleaned
        
        # Strategy 3: Inline JSON patterns
        json_patterns = [
            r'"edge_media_to_caption"\s*:\s*\{[^}]*"edges"\s*:\s*\[\s*\{[^}]*"node"\s*:\s*'
            r'\{[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
            r'"caption"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.){10,})"',
            r'"caption"\s*:\s*"((?:[^"\\]|\\.){10,})"',
        ]
        for pattern in json_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                cleaned = cls._clean_caption(match.group(1))
                if len(cleaned) > 10:
                    return cleaned
        
        # Strategy 4: Window data
        window_pattern = r'window\._sharedData\s*=\s*(\{.+?\});'
        match = re.search(window_pattern, text)
        if match:
            try:
                data = json.loads(match.group(1))
                caption = cls._recursive_search(data, {"caption", "text"})
                if caption:
                    cleaned = cls._clean_caption(caption)
                    if len(cleaned) > 10:
                        return cleaned
            except:
                pass
        
        return None
    
    @classmethod
    def _recursive_search(cls, obj: Any, target_keys: set, depth: int = 0) -> Optional[str]:
        """Recursively search for caption"""
        if depth > 8:
            return None
        
        if isinstance(obj, dict):
            for key in target_keys:
                if key in obj:
                    value = obj[key]
                    if isinstance(value, str) and len(value) > 10:
                        return value
                    elif isinstance(value, dict):
                        if "text" in value and isinstance(value["text"], str):
                            return value["text"]
            
            for value in obj.values():
                result = cls._recursive_search(value, target_keys, depth + 1)
                if result:
                    return result
        
        elif isinstance(obj, list):
            for item in obj:
                result = cls._recursive_search(item, target_keys, depth + 1)
                if result:
                    return result
        
        return None


# ══════════════════════════════════════════════
#  SMART HTML CAPTURE - Results First
# ══════════════════════════════════════════════

class HTMLCaptureManager:
    """Smart HTML capture with progressive timeout reduction"""
    
    @staticmethod
    async def _capture_attempt(page: Page, method: str, timeout: float) -> Optional[bytes]:
        """Single capture attempt"""
        try:
            if method == "evaluate":
                html = await asyncio.wait_for(
                    page.evaluate("document.documentElement.outerHTML"),
                    timeout=timeout
                )
            elif method == "content":
                html = await asyncio.wait_for(
                    page.content(),
                    timeout=timeout
                )
            else:
                return None
            
            if html and len(html) > 500:
                return html.encode('utf-8')
        except asyncio.TimeoutError:
            logger.debug(f"{method}() timeout after {timeout}s", indent=3)
        except Exception as e:
            logger.debug(f"{method}() error: {type(e).__name__}", indent=3)
        
        return None
    
    @classmethod
    async def capture_smart(cls, page: Page) -> Optional[bytes]:
        """
        Smart capture: tries multiple methods with decreasing timeouts
        Prioritizes getting SOME result over failing fast
        """
        for i, (method, timeout) in enumerate(HTML_CAPTURE_ATTEMPTS, 1):
            # Check shutdown
            if shutdown_manager.is_shutting_down():
                logger.debug("Shutdown - aborting capture", indent=3)
                return None
            
            logger.debug(f"Attempt {i}: {method}({timeout}s)", indent=2)
            
            html_bytes = await cls._capture_attempt(page, method, timeout)
            
            if html_bytes:
                size_kb = len(html_bytes) / 1024
                logger.debug(f"✓ Captured {size_kb:.1f}KB via {method}", indent=2)
                return html_bytes
            
            # Small delay between attempts
            if i < len(HTML_CAPTURE_ATTEMPTS):
                await asyncio.sleep(0.2)
        
        logger.warning("All capture attempts failed", indent=2)
        return None


# ══════════════════════════════════════════════
#  BALANCED SCRAPER
# ══════════════════════════════════════════════

class InstagramCaptionScraper2026:
    """Balanced scraper: get results, avoid infinite hangs"""
    
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        ]
        self._browser_alive = False
        self._context_alive = False
    
    async def dismiss_popups(self, page: Page):
        """Quick popup dismissal"""
        try:
            await asyncio.wait_for(self._dismiss_popups_internal(page), timeout=POPUP_TIMEOUT)
        except:
            pass
    
    async def _dismiss_popups_internal(self, page: Page):
        selectors = [
            'button:has-text("Not now")',
            'button:has-text("Not Now")',
            'button:has-text("Accept")',
        ]
        for sel in selectors:
            try:
                locator = page.locator(sel).first
                if await locator.is_visible(timeout=300):
                    await locator.click(timeout=500)
                    await asyncio.sleep(0.15)
            except:
                pass
    
    async def _wait_for_content(self, page: Page):
        """Wait for content to appear"""
        selectors = ['article', 'main', '[role="main"]', 'img', 'h2']
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=1500, state="attached")
                logger.debug(f"Found {sel}", indent=2)
                return
            except:
                continue
        # Fallback wait
        await asyncio.sleep(0.5)
    
    async def _settle_after_nav(self, page: Page):
        """Let page settle after navigation"""
        try:
            # Try networkidle but don't wait forever
            await page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
            logger.debug("Network idle", indent=2)
        except:
            # Fixed wait if networkidle times out
            logger.debug(f"Settling {POST_NAV_SETTLE_TIME}s", indent=2)
            await asyncio.sleep(POST_NAV_SETTLE_TIME)
    
    async def safe_goto(self, page: Page, url: str) -> bool:
        """Navigation with reasonable timeout"""
        try:
            response = await asyncio.wait_for(
                page.goto(url, wait_until=WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT),
                timeout=NAVIGATION_HARD_LIMIT
            )
            
            if response is None:
                logger.debug("No response", indent=2)
                return False
            
            if response.status >= 400:
                logger.debug(f"HTTP {response.status}", indent=2)
                return False
            
            # Check for blocks
            current_url = page.url
            if any(x in current_url for x in ["challenge", "checkpoint", "accounts/login"]):
                logger.warning(f"Blocked: {current_url}", indent=2)
                return False
            
            # Wait for content
            await self._wait_for_content(page)
            
            # Let it settle
            await self._settle_after_nav(page)
            
            logger.debug("Navigation complete", indent=2)
            return True
            
        except asyncio.TimeoutError:
            logger.warning(f"Nav timeout ({NAVIGATION_HARD_LIMIT}s)", indent=2)
            # Try to salvage - maybe partial load has content
            try:
                await self._wait_for_content(page)
                logger.debug("Partial load - proceeding", indent=2)
                return True
            except:
                return False
        except Exception as e:
            logger.debug(f"Nav error: {type(e).__name__}", indent=2)
            return False
    
    async def scrape_single_post(
        self,
        page: Page,
        post_url: str,
        shortcode: str,
        post_index: int,
    ) -> ScrapingResult:
        """Scrape with bounded timeout"""
        try:
            return await asyncio.wait_for(
                self._scrape_single_post_internal(page, post_url, shortcode, post_index),
                timeout=PER_POST_TIMEOUT
            )
        except asyncio.TimeoutError:
            self.logger.warning(f"[{post_index:>2}] Post timeout ({PER_POST_TIMEOUT}s) - skipping", indent=1)
            return ScrapingResult(success=False, error=f"{PER_POST_TIMEOUT}s timeout")
        except Exception as e:
            self.logger.error(f"[{post_index:>2}] {type(e).__name__}", indent=1)
            return ScrapingResult(success=False, error=str(e)[:50])
    
    async def _scrape_single_post_internal(
        self,
        page: Page,
        post_url: str,
        shortcode: str,
        post_index: int,
    ) -> ScrapingResult:
        """Internal scraping logic"""
        t0 = time.monotonic()
        post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"
        
        self.logger.info(f"[{post_index:>2}] {post_type} {shortcode}", indent=1)
        
        # Check shutdown
        if shutdown_manager.is_shutting_down():
            self.logger.warning("Shutdown - skipping", indent=2)
            return ScrapingResult(success=False, error="Shutdown")
        
        # Check browser
        if not self._browser_alive or not self._context_alive:
            return ScrapingResult(success=False, error="Browser closed")
        
        try:
            # Navigate
            logger.debug("Navigating...", indent=2)
            nav_success = await self.safe_goto(page, post_url)
            
            if not nav_success:
                elapsed = time.monotonic() - t0
                self.logger.warning(f"[{post_index:>2}] Nav failed {elapsed:.1f}s", indent=1)
                return ScrapingResult(success=False, error="Nav failed")
            
            # Dismiss popups
            await self.dismiss_popups(page)
            
            # Small wait for React to hydrate
            await asyncio.sleep(0.4)
            
            # Capture HTML with smart retries
            logger.debug("Capturing HTML...", indent=2)
            html_bytes = await HTMLCaptureManager.capture_smart(page)
            
            # Parse caption
            caption = None
            if html_bytes:
                caption = InstagramCaptionParser.parse(html_bytes, shortcode)
            
            elapsed = time.monotonic() - t0
            
            if caption:
                self.logger.success(
                    f"[{post_index:>2}] ✓ {shortcode:<12} {len(caption):>3}ch {elapsed:>5.1f}s",
                    indent=1,
                )
                return ScrapingResult(
                    success=True,
                    data={"url": post_url, "shortcode": shortcode, "caption": caption.strip()},
                )
            
            # No caption found
            self.logger.warning(f"[{post_index:>2}] no caption {elapsed:.1f}s", indent=1)
            return ScrapingResult(
                success=True,
                data={"url": post_url, "shortcode": shortcode, "caption": ""}
            )
            
        except Exception as e:
            elapsed = time.monotonic() - t0
            self.logger.error(f"[{post_index:>2}] {type(e).__name__} {elapsed:.1f}s", indent=1)
            return ScrapingResult(success=False, error=str(e)[:50])
    
    async def _collect_post_urls(self, page: Page, post_limit: int) -> List[str]:
        """Collect post URLs"""
        post_urls: List[str] = []
        last_height: int = 0
        stale_rounds: int = 0
        MAX_STALE = 3
        MAX_SCROLLS = 12
        
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
            if shutdown_manager.is_shutting_down():
                self.logger.warning("Shutdown during collection", indent=2)
                break
            
            try:
                links = await asyncio.wait_for(page.evaluate(js_collect), timeout=4.0) or []
            except:
                links = []
            
            full_links = [
                f"https://www.instagram.com{link}" if not link.startswith('http') else link
                for link in links
            ]
            
            new = [u for u in full_links if u not in post_urls]
            post_urls.extend(new)
            
            if new:
                self.logger.info(f"Scroll {i+1:>2}  +{len(new)} → {len(post_urls)}", indent=2)
                stale_rounds = 0
            else:
                stale_rounds += 1
            
            if len(post_urls) >= post_limit:
                break
            
            if stale_rounds >= MAX_STALE:
                self.logger.debug(f"No new posts for {MAX_STALE} scrolls", indent=2)
                break
            
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(SCROLL_DELAY)
                
                new_height = await asyncio.wait_for(
                    page.evaluate("document.body.scrollHeight"),
                    timeout=2.0
                ) or last_height
                
                if new_height == last_height:
                    stale_rounds += 1
                else:
                    last_height = new_height
                    stale_rounds = 0
            except:
                break
        
        return post_urls[:post_limit]
    
    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        """Main workflow"""
        t_total = time.monotonic()
        
        # Setup signal handlers
        setup_signal_handlers()
        
        self.logger.phase(
            "Instagram Scraper 2026 BALANCED",
            f"@{username}  ·  limit {post_limit}  ·  ~{PER_POST_TIMEOUT}s/post",
        )
        
        browser = None
        context = None
        
        try:
            async with async_playwright() as p:
                if shutdown_manager.is_shutting_down():
                    return []
                
                self.logger.section("Browser")
                
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
                self._browser_alive = True
                self.logger.success("Browser ready", indent=2)
                self.logger.section_end()
                
                context = await browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                    timezone_id="America/New_York",
                    extra_http_headers=INSTAGRAM_HEADERS,
                )
                self._context_alive = True
                await context.add_cookies(self.cookies)
                
                # Load profile
                self.logger.phase("Load Profile", f"@{username}")
                
                if shutdown_manager.is_shutting_down():
                    return []
                
                self.logger.section("Navigation")
                profile_page = await context.new_page()
                await profile_page.route("**/*", smart_route_handler)
                
                profile_url = f"https://www.instagram.com/{username}/"
                
                if not await self.safe_goto(profile_page, profile_url):
                    self.logger.error("Profile load failed", indent=1)
                    await profile_page.close()
                    return []
                
                if any(x in profile_page.url for x in ["challenge", "checkpoint", "accounts/login"]):
                    self.logger.error("Access blocked", indent=1)
                    await profile_page.close()
                    return []
                
                self.logger.success("Profile loaded", indent=2)
                self.logger.section_end()
                
                await self.dismiss_popups(profile_page)
                await asyncio.sleep(0.8)
                
                # Discover posts
                self.logger.phase("Discover Posts", f"Target: {post_limit}")
                
                if shutdown_manager.is_shutting_down():
                    await profile_page.close()
                    return []
                
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
                
                scrape_page = await context.new_page()
                await scrape_page.route("**/*", smart_route_handler)
                
                for i, url in enumerate(post_urls, 1):
                    # Check shutdown before each post
                    if shutdown_manager.is_shutting_down():
                        self.logger.warning(f"🛑 Shutdown at {i}/{len(post_urls)} - stopping", indent=1)
                        break
                    
                    sc = url.split("/p/")[-1].split("/reel/")[-1].split("/tv/")[-1].split("/")[0].split("?")[0]
                    
                    result = await self.scrape_single_post(scrape_page, url, sc, i)
                    
                    if result.success:
                        posts.append(result.data)
                    
                    # Delay between posts
                    if i < len(post_urls) and not shutdown_manager.is_shutting_down():
                        await asyncio.sleep(random.uniform(1.0, 1.8))
                    
                    self.logger.progress(i, len(post_urls), f"{len(posts)} ok")
                
                # Close page
                try:
                    await scrape_page.close()
                except:
                    pass
                
                self.logger.section_end()
                
                # Summary
                elapsed_total = time.monotonic() - t_total
                self.logger.phase("Summary", f"Total: {elapsed_total:.1f}s")
                self.logger.separator()
                self.logger.success(f"Scraped:  {len(posts)}/{len(post_urls)}", indent=1)
                
                captioned = sum(1 for p in posts if p.get("caption"))
                self.logger.info(f"Captions: {captioned}/{len(posts)}", indent=1)
                
                if posts and len(post_urls) > 0:
                    self.logger.info(f"Speed:    {elapsed_total/len(post_urls):.1f}s/post", indent=1)
                
                if shutdown_manager.is_shutting_down():
                    self.logger.warning("⚠️  Partial results due to shutdown", indent=1)
                
                self.logger.separator()
                
                return posts
                
        except Exception as e:
            import traceback
            self.logger.error(f"Fatal: {type(e).__name__}", indent=1)
            self.logger.debug(str(e)[:100], indent=2)
            return []
            
        finally:
            self._context_alive = False
            self._browser_alive = False
            
            self.logger.section("Cleanup")
            
            # Cleanup with reasonable timeout
            if context:
                try:
                    await asyncio.wait_for(context.close(), timeout=5.0)
                    self.logger.debug("Context closed", indent=2)
                except Exception:
                    self.logger.debug("Context close timeout", indent=2)
            
            if browser:
                try:
                    await asyncio.wait_for(browser.close(), timeout=5.0)
                    self.logger.success("Browser closed", indent=2)
                except Exception:
                    self.logger.debug("Browser close timeout", indent=2)
            
            self.logger.section_end()


# ══════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════

async def fetch_ig_urls(
    account: str,
    cookies: List[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Balanced scraper: get results, avoid hangs"""
    account = account.lstrip("@")
    
    logger.phase("fetch_ig_urls BALANCED", f"@{account}")
    logger.section("Cookie setup")
    
    if cookies is None:
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
    if not session_ok:
        logger.warning("sessionid missing", indent=2)
    
    logger.section_end()
    
    scraper = InstagramCaptionScraper2026(cookies=cookies, logger=logger)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )