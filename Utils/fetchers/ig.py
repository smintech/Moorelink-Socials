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
#  2026 CONFIGURATION
# ══════════════════════════════════════════════

NAVIGATION_TIMEOUT = 45_000  # Increased for grace
GRAPHQL_TIMEOUT = 20.0  # Softer
HTML_CAPTURE_TIMEOUT = 20.0  # Softer

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
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
}

# GraphQL doc_ids
PROFILE_POSTS_DOC_ID = "9310670392322965"
USER_INFO_DOC_ID = "2398832706970914"

# Logging unchanged
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


# Result dataclass unchanged
@dataclass
class ScrapingResult:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None


# Route Interceptor unchanged
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
    except Exception:
        await route.abort()


# Caption Parser unchanged
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


# Scraper with improvements
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
                if await locator.is_visible(timeout=800):
                    await locator.click(force=True, timeout=1500)
                    await asyncio.sleep(0.3)
            except:
                pass
    
    async def _capture_html(self, page: Page) -> bytes:
        max_attempts = 3  # Reduced for speed
        for attempt in range(1, max_attempts + 1):
            try:
                # Soft check for ready
                try:
                    await page.wait_for_function("() => document.readyState === 'complete'", timeout=5000)
                except PlaywrightTimeoutError:
                    self.logger.debug("readyState not complete - proceeding", indent=2)
                
                html = await asyncio.wait_for(
                    page.content(),
                    timeout=HTML_CAPTURE_TIMEOUT
                )
                if len(html) > 5000:
                    self.logger.debug(f"HTML captured ({len(html)/1024:.1f} KB, attempt {attempt})", indent=2)
                    return html.encode('utf-8')
                self.logger.debug(f"HTML small, retry {attempt}", indent=2)
            except Exception as e:
                self.logger.debug(f"content() failed attempt {attempt}: {type(e).__name__}", indent=2)
            await asyncio.sleep(random.uniform(1.5, 3.0))
        
        # Fallback
        try:
            html = await page.evaluate("document.documentElement.outerHTML")
            self.logger.debug("Fallback evaluate OK", indent=2)
            return html.encode('utf-8')
        except Exception as e:
            self.logger.debug(f"HTML capture failed: {type(e).__name__}", indent=2)
            return b""
    
    async def _wait_for_content(self, page: Page):
        selectors = ['article', 'header', 'main', 'h1', 'h2', 'img', 'video']
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=5000)  # Softer
                return
            except:
                continue
        await asyncio.sleep(1.5)
    
    async def safe_goto(self, page: Page, url: str) -> bool:
        try:
            response = await asyncio.wait_for(
                page.goto(url, wait_until=WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT),
                timeout=(NAVIGATION_TIMEOUT / 1000) + 5
            )
            
            if response is None or response.status >= 400:
                return False
            
            current_url = page.url
            if any(x in current_url for x in ["challenge", "checkpoint", "accounts/login"]):
                return False
            
            try:
                load_state = "networkidle" if any(p in url for p in ("/p/", "/reel/", "/tv/")) else "domcontentloaded"
                await page.wait_for_load_state(load_state, timeout=30000)  # Longer, softer
            except PlaywrightTimeoutError:
                self.logger.debug("Load state timeout - proceeding", indent=2)
            
            return True
        except Exception as e:
            self.logger.debug(f"Nav error: {type(e).__name__}", indent=2)
            return False
    
    async def scrape_single_post_html(self, page: Page, post_url: str, shortcode: str, post_index: int) -> ScrapingResult:
        try:
            t0 = time.monotonic()
            post_type = "REEL" if any(p in post_url for p in SLOW_PATH_PATTERNS) else "POST"
            self.logger.info(f"[{post_index:>2}] {post_type} {shortcode} (HTML)", indent=1)
            
            if not await self.safe_goto(page, post_url):
                return ScrapingResult(success=False, error="Nav failed")
            
            await self.dismiss_popups(page)
            await self._wait_for_content(page)
            if post_type == "REEL":
                await asyncio.sleep(random.uniform(2.0, 4.0))  # Softer for reels
            
            html_bytes = await self._capture_html(page)
            
            caption = None
            if len(html_bytes) > 1000:
                caption = InstagramCaptionParser.parse(html_bytes, shortcode)
            
            elapsed = time.monotonic() - t0
            
            if caption:
                self.logger.success(f"✓ {shortcode} {len(caption)} chars {elapsed:.1f}s", indent=1)
                return ScrapingResult(success=True, data={"url": post_url, "shortcode": shortcode, "caption": caption.strip()})
            else:
                self.logger.warning(f"No caption {shortcode} {elapsed:.1f}s", indent=1)
                return ScrapingResult(success=True, data={"url": post_url, "shortcode": shortcode, "caption": ""})
            
        except Exception as e:
            self.logger.error(f"HTML post error: {str(e)[:50]}", indent=1)
            return ScrapingResult(success=False, error=str(e))
    
    async def _collect_post_urls_html(self, page: Page, post_limit: int, shutdown_requested: bool) -> List[str]:
        post_urls: List[str] = []
        last_height = 0
        stale_rounds = 0
        MAX_STALE = 3
        MAX_SCROLLS = 20
        
        js_collect = """
        () => {
            const links = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]'));
            return [...new Set(links.map(a => a.href.split('?')[0]))];
        }
        """
        
        for i in range(MAX_SCROLLS):
            if shutdown_requested:
                self.logger.warning("Shutdown requested during scroll, stopping", indent=1)
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
            await asyncio.sleep(random.uniform(1.0, 2.5))
            
            new_height = await page.evaluate("document.body.scrollHeight") or last_height
            if new_height == last_height:
                stale_rounds += 1
            else:
                last_height = new_height
                stale_rounds = 0
        
        return post_urls[:post_limit]
    
    async def scrape_profile_html(self, page: Page, username: str, post_limit: int, shutdown_requested: Callable[[], bool]) -> List[Dict]:
        profile_url = f"https://www.instagram.com/{username}/"
        
        if not await self.safe_goto(page, profile_url):
            self.logger.error("Profile nav failed", indent=1)
            return []
        
        if any(x in page.url for x in ["challenge", "checkpoint", "login"]):
            self.logger.error("Access blocked", indent=1)
            return []
        
        await self.dismiss_popups(page)
        await asyncio.sleep(1.5)
        
        self.logger.section("Collect URLs")
        post_urls = await self._collect_post_urls_html(page, post_limit, shutdown_requested())
        self.logger.section_end(f"{len(post_urls)} found")
        
        if not post_urls:
            return []
        
        self.logger.section("Scrape posts")
        posts = []
        for i, url in enumerate(post_urls, 1):
            if shutdown_requested():
                self.logger.warning(f"Shutdown requested at post {i}, stopping", indent=1)
                break
            
            shortcode = url.split('/')[-2]
            
            try:
                result = await asyncio.wait_for(
                    self.scrape_single_post_html(page, url, shortcode, i),
                    timeout=180.0  # Soft per-post timeout (3 min)
                )
            except asyncio.TimeoutError:
                self.logger.error(f"Soft timeout 180s for post {i} - skipping", indent=1)
                continue  # Move to next
            
            if result.success:
                posts.append(result.data)
            
            await self._human_delay(1200, 3000)
        
        self.logger.section_end(f"{len(posts)} ok")
        return posts
    
    async def scrape_profile_api(self, context: BrowserContext, username: str, post_limit: int) -> List[Dict]:
        posts: List[Dict] = []
        
        try:
            profile_url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
            self.logger.debug(f"API profile: {profile_url}", indent=2)
            
            response = await context.request.get(
                profile_url,
                headers=INSTAGRAM_HEADERS,
                timeout=GRAPHQL_TIMEOUT * 1000
            )
            
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
                
                self.logger.debug(f"Paginate after={page_info['end_cursor']}", indent=2)
                
                pag_response = await context.request.get(
                    "https://www.instagram.com/graphql/query",
                    params=params,
                    headers=INSTAGRAM_HEADERS,
                    timeout=GRAPHQL_TIMEOUT * 1000
                )
                
                if not pag_response.ok:
                    raise ValueError(f"Pag {pag_response.status}")
                
                pag_data = await pag_response.json()
                pag_timeline = pag_data["data"]["user"]["edge_owner_to_timeline_media"]
                
                posts.extend(self._extract_posts(pag_timeline["edges"]))
                page_info = pag_timeline["page_info"]
                
                await self._human_delay()
            
            return posts[:post_limit]
            
        except Exception as e:
            self.logger.error(f"API error: {str(e)[:80]}", indent=1)
            return []
    
    def _extract_posts(self, edges: List[Dict]) -> List[Dict]:
        extracted = []
        for edge in edges:
            node = edge["node"]
            shortcode = node["shortcode"]
            url = f"https://www.instagram.com/p/{shortcode}/" if node["__typename"] != "GraphVideo" else f"https://www.instagram.com/reel/{shortcode}/"
            caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
            caption = caption_edges[0]["node"]["text"] if caption_edges else ""
            extracted.append({
                "url": url,
                "shortcode": shortcode,
                "caption": caption.strip()
            })
        return extracted
    
    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        t_total = time.monotonic()
        
        self.logger.phase("IG Scraper 2026", f"@{username} limit {post_limit} API+HTML")
        
        browser = None
        context = None
        page = None
        
        shutdown_requested = False
        def handle_sigterm():
            nonlocal shutdown_requested
            shutdown_requested = True
            self.logger.info("SIGTERM received - finishing current operations")
        
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
        loop.add_signal_handler(signal.SIGINT, handle_sigterm)
        
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
                    page = await context.new_page()
                    await page.route("**/*", smart_route_handler)
                    posts = await self.scrape_profile_html(page, username, post_limit, lambda: shutdown_requested)
                
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
                    self.logger.info(f"Speed {elapsed_total/len(posts):.1f}s/post" if posts else "N/A", indent=1)
                self.logger.separator()
                
                return posts
                
        except Exception as e:
            import traceback
            self.logger.error(f"Fatal {type(e).__name__}: {str(e)[:80]}", indent=1)
            self.logger.debug(traceback.format_exc(), indent=1)
            return []
            
        finally:
            self.logger.section("Cleanup")
            if page:
                await page.close()
            if context:
                await context.close()
            if browser:
                await browser.close()
                self.logger.success("Closed", indent=2)


# Public API unchanged
async def fetch_ig_urls(
    account: str,
    cookies: List[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    account = account.lstrip("@")
    
    logger.phase("fetch_ig_urls 2026", f"@{account}")
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