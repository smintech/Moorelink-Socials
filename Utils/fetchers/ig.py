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
from enum import Enum

# ─────────────────────────────────────────────
#  Config guard — works with or without Utils
# ─────────────────────────────────────────────
try:
    from Utils import config
except ImportError:
    class config:
        POST_LIMIT = 10


# ══════════════════════════════════════════════
#  Network Strategy Enums & Constants
# ══════════════════════════════════════════════

class WaitStrategy(Enum):
    DOMCONTENTLOADED = "domcontentloaded"
    LOAD             = "load"
    NETWORKIDLE      = "networkidle"
    COMMIT           = "commit"   # fastest — just first byte


# Progressive timeout ladder (ms) — faster first, patient last
TIMEOUT_LADDER = [10_000, 20_000, 35_000]

# Resources that are safe to block entirely
BLOCKED_RESOURCE_TYPES = {"font", "stylesheet", "media"}

# URL fragments that must NOT be blocked (CDN media we want)
CDN_ALLOWLIST = ("cdninstagram", "fbcdn")

# Instagram-specific "slow" paths that need more time
SLOW_PATH_PATTERNS = ("/reel/", "/tv/")


# ══════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════

class DetailedLogger:
    def __init__(self, name: str = "Instagram Scraper"):
        self.name = name
        self.step_count = 0
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s.%(msecs)03d] %(levelname)-8s | %(message)s",
            datefmt="%H:%M:%S",
        )
        self.logger = logging.getLogger(name)

    def step(self, title: str, details: str = ""):
        self.step_count += 1
        self.logger.info("=" * 70)
        self.logger.info(f"📍 STEP {self.step_count}: {title}")
        if details:
            self.logger.info(f"   {details}")
        self.logger.info("=" * 70)

    def info(self, msg: str, indent: int = 1):    self.logger.info   (f"{'   ' * indent}ℹ️  {msg}")
    def success(self, msg: str, indent: int = 1): self.logger.info   (f"{'   ' * indent}✅  {msg}")
    def warning(self, msg: str, indent: int = 1): self.logger.warning(f"{'   ' * indent}⚠️  {msg}")
    def error(self, msg: str, indent: int = 1):   self.logger.error  (f"{'   ' * indent}❌  {msg}")
    def debug(self, msg: str, indent: int = 1):   self.logger.debug  (f"{'   ' * indent}🐛  {msg}")


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
    Block heavy resources that we don't need while allowing CDN media.
    Images from Instagram/Facebook CDNs are explicitly allowed because
    we later read their URLs for the post media.
    """
    req  = route.request
    rtype = req.resource_type
    url   = req.url

    # Always allow CDN media (we need the URLs)
    if any(cdn in url for cdn in CDN_ALLOWLIST):
        await route.continue_()
        return

    # Block heavy-but-useless types
    if rtype in BLOCKED_RESOURCE_TYPES:
        await route.abort()
        return

    # Block analytics / tracking
    tracking_domains = (
        "google-analytics", "doubleclick", "facebook.net/en_US/fbevents",
        "connect.facebook.net", "scorecardresearch", "omtrdc.net",
    )
    if any(t in url for t in tracking_domains):
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

    def _is_slow_url(self, url: str) -> bool:
        return any(p in url for p in SLOW_PATH_PATTERNS)

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
        Multi-strategy navigation with progressive timeout ladder.

        Strategy order per attempt:
          1. domcontentloaded  (fast, sufficient for SPA hydration)
          2. load              (waits for window.onload)
          3. commit            (just first byte — last resort)

        Timeouts increase with each retry: 10s → 20s → 35s
        """
        # Give reels / TV extra base time
        timeout_base = TIMEOUT_LADDER[:]
        if self._is_slow_url(url):
            timeout_base = [t + 10_000 for t in timeout_base]

        wait_strategies: List[WaitStrategy] = [
            WaitStrategy.DOMCONTENTLOADED,
            WaitStrategy.LOAD,
            WaitStrategy.COMMIT,
        ]

        for attempt in range(max_retries):
            timeout   = timeout_base[min(attempt, len(timeout_base) - 1)]
            wait_strat = wait_strategies[min(attempt, len(wait_strategies) - 1)]

            self.logger.debug(
                f"Goto attempt {attempt + 1}/{max_retries} | "
                f"wait={wait_strat.value} timeout={timeout // 1000}s | {url[:60]}",
                indent=3,
            )

            try:
                response = await page.goto(
                    url,
                    wait_until=wait_strat.value,
                    timeout=timeout,
                )

                if response is None:
                    self.logger.warning("goto returned None response", indent=3)
                    continue

                if response.status < 400:
                    # For commit strategy, explicitly wait for DOM
                    if wait_strat == WaitStrategy.COMMIT:
                        try:
                            await page.wait_for_load_state(
                                "domcontentloaded", timeout=10_000
                            )
                        except Exception:
                            pass  # best-effort

                    self.logger.debug(
                        f"OK {response.status} after attempt {attempt + 1}", indent=3
                    )
                    return True

                if response.status in (429, 503):
                    wait = 3 + attempt * 2
                    self.logger.warning(
                        f"Rate-limited ({response.status}), waiting {wait}s", indent=3
                    )
                    await asyncio.sleep(wait)

            except PlaywrightError as e:
                err = str(e)
                is_timeout = "Timeout" in err or "timeout" in err
                is_net_err = "net::ERR_" in err

                self.logger.warning(
                    f"Attempt {attempt + 1} failed: {err[:80]}", indent=3
                )

                if is_timeout or is_net_err:
                    if attempt < max_retries - 1:
                        backoff = self._jitter(1.5, 3.0) * (attempt + 1)
                        self.logger.debug(f"Backing off {backoff:.1f}s …", indent=3)
                        await asyncio.sleep(backoff)
                        continue
                # Non-retriable error
                raise

        self.logger.error(f"All {max_retries} goto attempts failed for {url[:60]}", indent=2)
        return False

    # ------------------------------------------------------------------
    #  Lazy-load / hydration waiter
    # ------------------------------------------------------------------

    async def wait_for_content(self, page: Page, url: str):
        """
        After navigation, wait for Instagram's SPA to hydrate the feed.
        Uses a combination of:
          - Explicit selector polling (post article or caption area)
          - Scroll jitter to trigger lazy-loaded images
          - Fallback sleep if nothing visible within budget
        """
        selectors_to_await = [
            "article",
            "div[role='main']",
            "main",
            "section",
        ]

        budget_ms = 8_000 if self._is_slow_url(url) else 5_000

        for sel in selectors_to_await:
            try:
                await page.wait_for_selector(sel, timeout=budget_ms, state="attached")
                # Small jitter scroll to trigger lazy loaders
                await page.evaluate("window.scrollBy(0, 120)")
                await asyncio.sleep(0.25)
                await page.evaluate("window.scrollBy(0, -120)")
                return
            except Exception:
                pass  # try next selector

        # Last resort — just sleep and hope for the best
        await asyncio.sleep(1.5)

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
        headers = self._build_headers()
        variables = {
            "shortcode": shortcode,
            "fetch_tagged_user_count": None,
            "hoisted_comment_id": None,
            "hoisted_reply_id": None,
        }
        body = (
            f"variables={quote(json.dumps(variables, separators=(',', ':')))}"
            f"&doc_id={self.post_doc_id}"
        )

        # Escape single quotes for inline JS string
        safe_body = body.replace("'", "\\'")

        script = f"""
            () => new Promise((resolve) => {{
                const xhr = new XMLHttpRequest();
                xhr.open('POST', 'https://www.instagram.com/graphql/query', true);
                xhr.setRequestHeader('accept', '{headers["accept"]}');
                xhr.setRequestHeader('content-type', '{headers["content-type"]}');
                xhr.setRequestHeader('x-csrftoken', '{headers["x-csrftoken"]}');
                xhr.setRequestHeader('x-ig-app-id', '{headers["x-ig-app-id"]}');
                xhr.setRequestHeader('x-requested-with', 'XMLHttpRequest');
                xhr.setRequestHeader('referer', '{headers["referer"]}');
                xhr.timeout = 9000;
                xhr.onload  = () => {{
                    try {{ resolve(JSON.parse(xhr.responseText)); }}
                    catch (e) {{ resolve({{error: 'parse: ' + e.message}}); }}
                }};
                xhr.onerror   = () => resolve({{error: 'network'}});
                xhr.ontimeout = () => resolve({{error: 'timeout'}});
                xhr.send('{safe_body}');
            }})
        """
        try:
            result = await page.evaluate(script)
            if not result or "data" not in result:
                return None
            media = result["data"].get("xdt_shortcode_media", {})
            edges = media.get("edge_media_to_caption", {}).get("edges", [])
            if edges:
                return edges[0]["node"].get("text", "") or None
            return media.get("accessibility_caption") or None
        except Exception as e:
            self.logger.debug(f"GraphQL: {str(e)[:60]}", indent=3)
            return None

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 2: DOM
    # ------------------------------------------------------------------

    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        await asyncio.sleep(0.25)
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
        try:
            res = await page.evaluate(script)
            if res and res.get("text"):
                return re.sub(r"\s*more\s*$", "", res["text"], flags=re.IGNORECASE).strip()
        except Exception as e:
            self.logger.debug(f"DOM: {str(e)[:50]}", indent=3)
        return None

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 3: JSON-LD
    # ------------------------------------------------------------------

    async def extract_caption_from_ldjson(self, page: Page) -> Optional[str]:
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
        try:
            return await page.evaluate(script)
        except Exception:
            return None

    # ------------------------------------------------------------------
    #  Caption extraction — Strategy 4: window.__additionalData / shared
    # ------------------------------------------------------------------

    async def extract_caption_from_window_data(self, page: Page) -> Optional[str]:
        """Instagram sometimes embeds the full media object in window globals."""
        script = r"""
            () => {
                // Try __additionalData
                try {
                    const keys = Object.keys(window.__additionalData || {});
                    for (const k of keys) {
                        const media = window.__additionalData[k]?.data?.graphql?.shortcode_media
                                   || window.__additionalData[k]?.data?.xdt_shortcode_media;
                        if (media) {
                            const edges = media.edge_media_to_caption?.edges || [];
                            if (edges.length) return edges[0].node.text;
                        }
                    }
                } catch (_) {}
                // Try _sharedData
                try {
                    const media = window._sharedData?.entry_data?.PostPage?.[0]
                                   ?.graphql?.shortcode_media;
                    if (media) {
                        const edges = media.edge_media_to_caption?.edges || [];
                        if (edges.length) return edges[0].node.text;
                    }
                } catch (_) {}
                return null;
            }
        """
        try:
            return await page.evaluate(script)
        except Exception:
            return None

    # ------------------------------------------------------------------
    #  Master caption orchestrator
    # ------------------------------------------------------------------

    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
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
                    self.logger.debug(f"Caption via {name} ({len(result)} chars)", indent=3)
                    return result
            except Exception as e:
                self.logger.debug(f"{name} failed: {str(e)[:50]}", indent=3)
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

        try:
            url = await page.evaluate(script)
            if url and not url.startswith("blob"):
                return url, is_video
        except Exception as e:
            self.logger.debug(f"Media extract error: {str(e)[:50]}", indent=3)

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
        try:
            self.logger.info(f"[{post_index}] {shortcode}", indent=1)
            page = await context.new_page()
            page.set_default_navigation_timeout(40_000)
            page.set_default_timeout(15_000)

            # Smart resource blocking
            await page.route("**/*", smart_route_handler)

            # Navigate with full resilient retry stack
            if not await self.safe_goto(page, post_url, max_retries=3):
                return ScrapingResult(success=False, error="Navigation failed after all retries")

            if "accounts/login" in page.url:
                return ScrapingResult(success=False, error="Redirected to login")

            await self.dismiss_popups(page)
            await self.wait_for_content(page, post_url)

            caption   = await self.extract_caption_from_post(page, shortcode)
            media_url, is_video = await self.extract_media_from_post(page, post_url)

            if media_url and not media_url.startswith("blob"):
                self.logger.success(
                    f"{'🎥 VIDEO' if is_video else '📸 IMAGE'} | "
                    f"Caption: {len(caption)} chars",
                    indent=2,
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
            return ScrapingResult(success=False, error="No media URL found")

        except Exception as e:
            self.logger.error(f"Post error: {str(e)[:80]}", indent=2)
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
            links: List[str] = await page.evaluate(js_collect)
            new = [u for u in links if u not in post_urls]
            post_urls.extend(new)
            self.logger.debug(f"Scroll {i+1}: {len(post_urls)} posts collected", indent=2)

            if len(post_urls) >= post_limit:
                break

            # Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            # Wait for new content — poll for height change up to ~2s
            for _ in range(4):
                await asyncio.sleep(0.5)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height != last_height:
                    last_height = new_height
                    stale_rounds = 0
                    break
            else:
                stale_rounds += 1

            if stale_rounds >= MAX_STALE:
                self.logger.debug("Grid end reached (no more height change)", indent=2)
                break

        return post_urls[:post_limit]

    # ------------------------------------------------------------------
    #  Main entry point
    # ------------------------------------------------------------------

    async def scrape_profile(
        self, username: str, post_limit: int = 10
    ) -> List[Dict]:
        self.logger.step(
            "Initialize Scraper",
            f"Target: @{username} | Limit: {post_limit} | Workers: {self.max_concurrent}",
        )

        async with async_playwright() as p:
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
                    # Increase network timeouts at the Chrome level
                    "--dns-prefetch-disable",
                ],
            )
            self.logger.success("Browser launched", indent=1)

            # ── Profile context ──────────────────────────────────────
            main_ctx = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
                java_script_enabled=True,
                ignore_https_errors=True,          # tolerate CDN cert issues
            )
            await main_ctx.add_cookies(self.cookies)

            try:
                # ── Phase 1: Load profile ────────────────────────────
                self.logger.step("Load Profile", f"@{username}")
                profile_page = await main_ctx.new_page()
                await profile_page.route("**/*", smart_route_handler)

                profile_url = f"https://www.instagram.com/{username}/"
                if not await self.safe_goto(profile_page, profile_url, max_retries=3):
                    self.logger.error("Could not load profile page")
                    return []

                if "accounts/login" in profile_page.url:
                    self.logger.error("Redirected to login — cookies may be expired")
                    await profile_page.close()
                    return []

                await self.dismiss_popups(profile_page)
                await self.wait_for_content(profile_page, profile_url)

                # ── Phase 2: Discover posts ──────────────────────────
                self.logger.step("Discover Posts")
                post_urls = await self._collect_post_urls(profile_page, post_limit)
                await profile_page.close()

                self.logger.success(f"Found {len(post_urls)} post URLs", indent=1)
                if not post_urls:
                    return []

                # ── Phase 3: Concurrent scraping ─────────────────────
                self.logger.step(
                    "Scrape Posts",
                    f"{len(post_urls)} posts across {self.max_concurrent} workers",
                )

                # One isolated BrowserContext per worker prevents cross-task
                # "execution context destroyed" errors
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

                semaphore = asyncio.Semaphore(self.max_concurrent)

                def _shortcode(url: str, idx: int) -> str:
                    for segment in ("/p/", "/reel/", "/tv/"):
                        if segment in url:
                            return url.split(segment)[-1].split("/")[0]
                    return f"post_{idx}"

                async def _worker(idx: int, url: str) -> ScrapingResult:
                    async with semaphore:
                        ctx = worker_contexts[idx % self.max_concurrent]
                        sc  = _shortcode(url, idx)
                        return await self.scrape_single_post(ctx, url, sc, idx + 1)

                tasks   = [_worker(i, u) for i, u in enumerate(post_urls)]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                posts: List[Dict] = []
                for i, res in enumerate(results):
                    if isinstance(res, ScrapingResult) and res.success:
                        posts.append(res.data)
                    elif isinstance(res, Exception):
                        self.logger.error(f"Worker {i+1}: {str(res)[:60]}", indent=2)

                for wctx in worker_contexts:
                    try:
                        await wctx.close()
                    except Exception:
                        pass

                self.logger.step(
                    "Complete",
                    f"✅ {len(posts)}/{len(post_urls)} posts scraped successfully",
                )
                return posts

            except Exception as e:
                import traceback
                self.logger.error(f"Fatal: {str(e)[:80]}", indent=1)
                self.logger.debug(traceback.format_exc(), indent=2)
                return []
            finally:
                try:
                    await main_ctx.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                except Exception:
                    pass
                self.logger.success("Browser closed", indent=1)


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
    logger.step("Configuration", f"Account: @{account} | Workers: {max_concurrent}")

    if cookies is None:
        raw = os.getenv("IG_COOKIES", "")
        if not raw:
            logger.error("No IG_COOKIES environment variable set")
            return []
        try:
            cookies = json.loads(raw)
            logger.success(f"Loaded {len(cookies)} cookies from env", indent=1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid IG_COOKIES JSON: {e}")
            return []

    if not any(c.get("name") == "csrftoken" for c in cookies):
        logger.warning("No csrftoken in cookies — GraphQL strategy may degrade")

    scraper = InstagramScraper(cookies=cookies, logger=logger, max_concurrent=max_concurrent)
    return await scraper.scrape_profile(
        username=account,
        post_limit=getattr(config, "POST_LIMIT", 10),
    )