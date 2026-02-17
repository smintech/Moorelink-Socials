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
from dataclasses import dataclass
from Utils import config

@dataclass
class ScrapingResult:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None

class DetailedLogger:
    def __init__(self, name: str = "Instagram Scraper"):
        self.name = name
        self.step_count = 0
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s.%(msecs)03d] %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger(name)

    def step(self, title: str, details: str = ""):
        self.step_count += 1
        self.logger.info("=" * 70)
        self.logger.info(f"📍 STEP {self.step_count}: {title}")
        if details:
            self.logger.info(f"   {details}")
        self.logger.info("=" * 70)

    def info(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.info(f"{prefix}ℹ️ {message}")

    def success(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.info(f"{prefix}✅ {message}")

    def warning(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.warning(f"{prefix}⚠️ {message}")

    def error(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.error(f"{prefix}❌ {message}")

    def debug(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.debug(f"{prefix}🐛 {message}")

logger = DetailedLogger("Instagram Scraper")


class InstagramScraper:
    def __init__(self, cookies: List[Dict], logger: DetailedLogger, max_concurrent: int = 3):
        self.cookies = cookies
        self.logger = logger
        self.max_concurrent = max_concurrent
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.csrf_token = None
        self.ig_app_id = "936619743392459"
        self.post_doc_id = "8845758582119845"  # Updated doc_id for 2024-2025
        
        for cookie in cookies:
            if cookie['name'] == 'csrftoken':
                self.csrf_token = cookie['value']
                break

    def get_random_delay(self, min_sec=0.5, max_sec=1.5) -> float:
        """Conservative delays to avoid rate limiting"""
        return random.uniform(min_sec, max_sec)

    async def dismiss_popups(self, page: Page):
        """Quick popup dismissal with short timeout"""
        try:
            selectors = [
                'button:has-text("Not now")',
                'button:has-text("Allow all cookies")',
                'button:has-text("Accept")',
                '[role="dialog"] button:has-text("Not Now")',
            ]
            for selector in selectors:
                try:
                    elem = page.locator(selector).first
                    if await elem.is_visible(timeout=1000):
                        await elem.click(timeout=1000)
                        await asyncio.sleep(0.2)
                except:
                    pass
        except:
            pass

    async def safe_goto(self, page: Page, url: str, max_retries: int = 2) -> bool:
        """Safe navigation with retry logic and proper error handling"""
        for attempt in range(max_retries):
            try:
                # CRITICAL: Don't use asyncio.wait_for with page.goto - let Playwright handle timeouts
                response = await page.goto(
                    url, 
                    wait_until='domcontentloaded',
                    timeout=15000  # 15s timeout per attempt
                )
                
                if response and response.status < 400:
                    return True
                    
            except PlaywrightError as e:
                if "net::ERR_" in str(e) or "Timeout" in str(e):
                    self.logger.warning(f"Navigation failed (attempt {attempt + 1}): {str(e)[:50]}", indent=2)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(self.get_random_delay(1, 2))
                        continue
                raise
        
        return False

    async def get_headers(self) -> Dict[str, str]:
        """Get GraphQL headers with current CSRF token"""
        return {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.instagram.com',
            'referer': 'https://www.instagram.com/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': self.user_agents[0],
            'x-csrftoken': self.csrf_token or '',
            'x-ig-app-id': self.ig_app_id,
            'x-ig-www-claim': '0',
            'x-requested-with': 'XMLHttpRequest',
        }

    async def extract_caption_graphql(self, page: Page, shortcode: str) -> Optional[str]:
        """
        STRATEGY 1: GraphQL API via page.evaluate() - FIXED VERSION
        Critical fix: Don't use async/await inside page.evaluate()
        """
        try:
            headers = await self.get_headers()
            
            variables = {
                'shortcode': shortcode,
                'fetch_tagged_user_count': None,
                'hoisted_comment_id': None,
                'hoisted_reply_id': None
            }
            
            body = f"variables={quote(json.dumps(variables, separators=(',', ':')))}&doc_id={self.post_doc_id}"
            
            # CRITICAL FIX: Use synchronous fetch inside evaluate, no async/await
            script = f"""
                () => {{
                    return new Promise((resolve) => {{
                        const xhr = new XMLHttpRequest();
                        xhr.open('POST', 'https://www.instagram.com/graphql/query', true);
                        xhr.setRequestHeader('accept', '{headers['accept']}');
                        xhr.setRequestHeader('content-type', '{headers['content-type']}');
                        xhr.setRequestHeader('x-csrftoken', '{headers['x-csrftoken']}');
                        xhr.setRequestHeader('x-ig-app-id', '{headers['x-ig-app-id']}');
                        xhr.setRequestHeader('x-requested-with', 'XMLHttpRequest');
                        xhr.setRequestHeader('referer', '{headers['referer']}');
                        
                        xhr.onload = function() {{
                            if (xhr.status === 200) {{
                                try {{
                                    const data = JSON.parse(xhr.responseText);
                                    resolve(data);
                                }} catch (e) {{
                                    resolve({{error: 'Parse error: ' + e.message}});
                                }}
                            }} else {{
                                resolve({{error: 'HTTP ' + xhr.status}});
                            }}
                        }};
                        
                        xhr.onerror = () => resolve({{error: 'Network error'}});
                        xhr.ontimeout = () => resolve({{error: 'Timeout'}});
                        xhr.timeout = 8000;
                        
                        xhr.send('{body}');
                    }});
                }}
            """
            
            # Use page.evaluate with timeout, NOT asyncio.wait_for
            result = await page.evaluate(script)
            
            if result and 'data' in result and 'xdt_shortcode_media' in result['data']:
                media = result['data']['xdt_shortcode_media']
                
                # Extract caption from edges
                caption_edges = media.get('edge_media_to_caption', {}).get('edges', [])
                if caption_edges and len(caption_edges) > 0:
                    caption_text = caption_edges[0]['node'].get('text', '')
                    if caption_text:
                        return caption_text
                
                # Fallback to accessibility caption
                alt_text = media.get('accessibility_caption', '')
                if alt_text:
                    return alt_text
            
            return None
            
        except Exception as e:
            self.logger.debug(f"GraphQL error: {str(e)[:60]}", indent=3)
            return None

    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        """
        STRATEGY 2: DOM extraction - Multiple fallback selectors
        """
        # Wait a moment for any lazy-loaded content
        await asyncio.sleep(0.3)
        
        script = r"""
            () => {
                const strategies = [];
                
                // Strategy A: Instagram's specific caption classes (2024-2025)
                const captionSelectors = [
                    'div._aacl._a9zr._a9zo._a9z9',
                    'div._aacl._a9zr',
                    'div[data-testid="post-caption"]',
                    'article div[role="button"] + div span',
                    'h1 + div span'
                ];
                
                for (const selector of captionSelectors) {
                    const el = document.querySelector(selector);
                    if (el) {
                        const text = el.innerText?.trim();
                        if (text && text.length > 5) {
                            strategies.push({text: text, source: selector});
                            break;
                        }
                    }
                }
                
                // Strategy B: Meta/OpenGraph description
                const metaDesc = document.querySelector('meta[property="og:description"]');
                if (metaDesc) {
                    const content = metaDesc.getAttribute('content');
                    if (content && content.length > 20) {
                        // Clean up "Username on Instagram: " prefix
                        const cleaned = content.replace(/^[^,]+,\s*/, '').trim();
                        if (cleaned.length > 20) {
                            strategies.push({text: cleaned, source: 'meta_og'});
                        }
                    }
                }
                
                // Strategy C: Article text content
                const article = document.querySelector('article');
                if (article) {
                    const text = article.innerText?.trim();
                    if (text && text.length > 30 && text.length < 10000) {
                        strategies.push({text: text, source: 'article'});
                    }
                }
                
                if (strategies.length > 0) {
                    // Prefer longer, more detailed captions
                    strategies.sort((a, b) => b.text.length - a.text.length);
                    return strategies[0];
                }
                
                return null;
            }
        """
        
        try:
            result = await page.evaluate(script)
            if result and result['text']:
                caption = result['text']
                # Clean up "more" button text
                caption = re.sub(r'\s*more\s*$', '', caption, flags=re.IGNORECASE).strip()
                return caption
        except Exception as e:
            self.logger.debug(f"DOM extraction error: {str(e)[:50]}", indent=3)
        
        return None

    async def extract_caption_from_ldjson(self, page: Page) -> Optional[str]:
        """
        STRATEGY 3: Extract from JSON-LD structured data
        """
        script = r"""
            () => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (const script of scripts) {
                    try {
                        const data = JSON.parse(script.textContent);
                        if (data.caption || data.description) {
                            return data.caption || data.description;
                        }
                        // Check for VideoObject or ImageObject
                        if (data['@type'] === 'VideoObject' || data['@type'] === 'ImageObject') {
                            return data.description || data.caption;
                        }
                    } catch (e) {
                        continue;
                    }
                }
                return null;
            }
        """
        
        try:
            return await page.evaluate(script)
        except:
            return None

    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
        """
        Master caption extraction with sequential strategies and error isolation
        """
        caption = ""
        
        # Strategy 1: GraphQL API (most reliable for public posts)
        if shortcode:
            try:
                caption = await self.extract_caption_graphql(page, shortcode)
                if caption:
                    return caption
            except Exception as e:
                self.logger.debug(f"GraphQL failed: {str(e)[:40]}", indent=2)
        
        # Strategy 2: DOM extraction
        try:
            caption = await self.extract_caption_from_dom(page)
            if caption:
                return caption
        except Exception as e:
            self.logger.debug(f"DOM extraction failed: {str(e)[:40]}", indent=2)
        
        # Strategy 3: JSON-LD
        try:
            caption = await self.extract_caption_from_ldjson(page)
            if caption:
                return caption
        except Exception as e:
            self.logger.debug(f"JSON-LD failed: {str(e)[:40]}", indent=2)
        
        return caption

    async def extract_media_from_post(self, page: Page, post_url: str) -> Tuple[str, bool]:
        """
        Extract media URL with multiple fallback strategies
        """
        is_video = '/reel/' in post_url or '/tv/' in post_url
        
        if is_video:
            # For videos, check multiple sources
            script = r"""
                () => {
                    // Check video element
                    const video = document.querySelector('video[src]');
                    if (video?.src && !video.src.startsWith('blob')) return video.src;
                    
                    // Check source element
                    const source = document.querySelector('video source[src]');
                    if (source?.src && !source.src.startsWith('blob')) return source.src;
                    
                    // Check meta tags
                    const metaVideo = document.querySelector('meta[property="og:video"]');
                    if (metaVideo) return metaVideo.getAttribute('content');
                    
                    // Check for video in _sharedData
                    if (window._sharedData) {
                        try {
                            const media = window._sharedData.entry_data.PostPage[0].graphql.shortcode_media;
                            if (media.video_url) return media.video_url;
                        } catch (e) {}
                    }
                    
                    return '';
                }
            """
        else:
            # For images
            script = r"""
                () => {
                    // Meta tag (highest quality)
                    const metaImg = document.querySelector('meta[property="og:image"]');
                    if (metaImg) {
                        const content = metaImg.getAttribute('content');
                        if (content && content.includes('cdninstagram')) return content;
                    }
                    
                    // Large images in article
                    const imgs = document.querySelectorAll('article img[src]');
                    const candidates = [];
                    
                    for (const img of imgs) {
                        const src = img.src;
                        if ((src.includes('cdninstagram') || src.includes('fbcdn')) && 
                            !src.includes('profile') && 
                            img.naturalWidth > 400) {
                            candidates.push({src: src, width: img.naturalWidth});
                        }
                    }
                    
                    if (candidates.length > 0) {
                        candidates.sort((a, b) => b.width - a.width);
                        return candidates[0].src;
                    }
                    
                    return '';
                }
            """
        
        try:
            media_url = await page.evaluate(script)
            if media_url and not media_url.startswith('blob'):
                return media_url, is_video
        except Exception as e:
            self.logger.debug(f"Media extraction error: {str(e)[:40]}", indent=3)
        
        return "", is_video

    async def scrape_single_post(self, context: BrowserContext, post_url: str, shortcode: str, post_index: int) -> ScrapingResult:
        """
        Scrape a single post in complete isolation (dedicated page per task)
        This prevents execution context destroyed errors
        """
        page = None
        try:
            self.logger.info(f"[{post_index}] Processing: {shortcode}", indent=1)
            
            # Create dedicated page for this task
            page = await context.new_page()
            page.set_default_navigation_timeout(20000)
            page.set_default_timeout(10000)
            
            # Block unnecessary resources to speed up loading
            await page.route("**/*", lambda route: (
                route.abort() if route.request.resource_type in ['image', 'font', 'media'] 
                and not any(x in route.request.url for x in ['cdninstagram', 'fbcdn'])
                else route.continue_()
            ))
            
            # Navigate with retry logic
            success = await self.safe_goto(page, post_url, max_retries=2)
            if not success:
                return ScrapingResult(success=False, error="Navigation failed after retries")
            
            # Check for login redirect
            if 'accounts/login' in page.url:
                return ScrapingResult(success=False, error="Redirected to login")
            
            # Dismiss popups
            await self.dismiss_popups(page)
            
            # Wait for content to stabilize
            await asyncio.sleep(0.5)
            
            # Extract data
            caption = await self.extract_caption_from_post(page, shortcode)
            media_url, is_video = await self.extract_media_from_post(page, post_url)
            
            if media_url and not media_url.startswith('blob'):
                post_data = {
                    "url": post_url,
                    "shortcode": shortcode,
                    "caption": caption,
                    "media_url": media_url,
                    "is_video": is_video,
                }
                self.logger.success(
                    f"{'🎥 VIDEO' if is_video else '📸 IMAGE'} | Caption: {len(caption)} chars", 
                    indent=2
                )
                return ScrapingResult(success=True, data=post_data)
            else:
                return ScrapingResult(success=False, error="No media URL found")
                
        except Exception as e:
            error_msg = str(e)[:60]
            self.logger.error(f"Post error: {error_msg}", indent=2)
            return ScrapingResult(success=False, error=error_msg)
            
        finally:
            # CRITICAL: Always close the page to free resources
            if page:
                try:
                    await page.close()
                except:
                    pass

    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        self.logger.step(
            "Initialize Scraper", 
            f"Target: @{username} | Limit: {post_limit} | Concurrent: {self.max_concurrent}"
        )
        
        async with async_playwright() as p:
            self.logger.info("Launching browser...", indent=1)
            
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080'
                ]
            )
            
            self.logger.success("Browser launched", indent=1)
            
            # Create main context for profile browsing
            context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            await context.add_cookies(self.cookies)
            self.logger.success(f"Cookies loaded: {len(self.cookies)}", indent=1)
            
            try:
                # === PHASE 1: LOAD PROFILE ===
                self.logger.step("Load Profile", f"https://www.instagram.com/{username}/")
                
                profile_page = await context.new_page()
                
                profile_url = f"https://www.instagram.com/{username}/"
                
                if not await self.safe_goto(profile_page, profile_url, max_retries=2):
                    self.logger.error("Failed to load profile")
                    return []
                
                if 'accounts/login' in profile_page.url:
                    self.logger.error("Redirected to login - cookies expired")
                    await profile_page.close()
                    return []
                
                await self.dismiss_popups(profile_page)
                await asyncio.sleep(0.5)
                
                # === PHASE 2: DISCOVER POSTS ===
                self.logger.step("Discover Posts", "Scrolling to load post grid...")
                
                post_urls = []
                last_height = 0
                no_change_count = 0
                
                for scroll_iter in range(10):  # Max 10 scrolls
                    # Extract links currently visible
                    script = r"""
                        () => {
                            const links = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]'));
                            return [...new Set(links.map(a => a.href.split('?')[0]))];
                        }
                    """
                    
                    current_links = await profile_page.evaluate(script)
                    new_links = [url for url in current_links if url not in post_urls]
                    post_urls.extend(new_links)
                    
                    self.logger.debug(f"Scroll {scroll_iter + 1}: Found {len(post_urls)} total posts", indent=2)
                    
                    if len(post_urls) >= post_limit:
                        break
                    
                    # Scroll down
                    await profile_page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(0.6)  # Slightly longer for grid loading
                    
                    # Check if we've reached the end
                    new_height = await profile_page.evaluate('document.body.scrollHeight')
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 2:
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height
                
                post_urls = post_urls[:post_limit]
                await profile_page.close()
                
                self.logger.success(f"Found {len(post_urls)} unique posts", indent=1)
                
                if not post_urls:
                    return []
                
                # === PHASE 3: CONCURRENT POST SCRAPING ===
                self.logger.step(
                    "Scrape Posts", 
                    f"Processing {len(post_urls)} posts with {self.max_concurrent} workers"
                )
                
                # Create worker contexts for true isolation
                # This prevents execution context destroyed errors
                worker_contexts = []
                for i in range(self.max_concurrent):
                    worker_ctx = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        viewport={'width': 1920, 'height': 1080},
                        locale='en-US'
                    )
                    await worker_ctx.add_cookies(self.cookies)
                    worker_contexts.append(worker_ctx)
                
                # Create semaphore for controlled concurrency
                semaphore = asyncio.Semaphore(self.max_concurrent)
                
                async def scrape_with_worker(idx: int, url: str) -> ScrapingResult:
                    async with semaphore:
                        worker_idx = idx % self.max_concurrent
                        shortcode = (
                            url.split('/p/')[-1].split('/')[0] or 
                            url.split('/reel/')[-1].split('/')[0] or
                            url.split('/tv/')[-1].split('/')[0] or
                            f"post_{idx}"
                        )
                        
                        return await self.scrape_single_post(
                            worker_contexts[worker_idx], 
                            url, 
                            shortcode, 
                            idx + 1
                        )
                
                # Execute all tasks with controlled concurrency
                tasks = [
                    scrape_with_worker(idx, url) 
                    for idx, url in enumerate(post_urls)
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                posts = []
                for i, result in enumerate(results):
                    if isinstance(result, ScrapingResult) and result.success:
                        posts.append(result.data)
                    elif isinstance(result, Exception):
                        self.logger.error(f"Task {i+1} exception: {str(result)[:50]}", indent=2)
                
                # Cleanup worker contexts
                for ctx in worker_contexts:
                    await ctx.close()
                
                self.logger.step(
                    "Scraping Complete", 
                    f"✅ Successfully scraped {len(posts)}/{len(post_urls)} posts"
                )
                return posts
                
            except Exception as e:
                self.logger.error(f"Fatal error: {str(e)[:60]}", indent=1)
                import traceback
                self.logger.debug(traceback.format_exc(), indent=2)
                return []
            
            finally:
                self.logger.info("Closing browser...", indent=1)
                await browser.close()
                self.logger.success("Cleanup complete", indent=1)


async def fetch_ig_urls(account: str, cookies: List[Dict[str, Any]] = None, max_concurrent: int = 3) -> List[Dict[str, Any]]:
    """
    Returns:
        List of post dictionaries with url, caption, media_url, is_video
    """
    account = account.lstrip('@')
    
    logger.step("Configuration", f"Max concurrent workers: {max_concurrent}")
    
    if cookies is None:
        cookies_str = os.getenv('IG_COOKIES')
        if not cookies_str:
            logger.error("No IG_COOKIES environment variable set")
            return []
        try:
            cookies = json.loads(cookies_str)
            logger.success(f"Loaded {len(cookies)} cookies from env")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid IG_COOKIES JSON: {str(e)}")
            return []
    
    # Validate cookies have required fields
    if not any(c.get('name') == 'csrftoken' for c in cookies):
        logger.warning("No csrftoken found in cookies - GraphQL API may fail")
    
    scraper = InstagramScraper(
        cookies=cookies, 
        logger=logger, 
        max_concurrent=max_concurrent
    )
    
    posts = await scraper.scrape_profile(
        username=account, 
        post_limit=config.POST_LIMIT if hasattr(config, 'POST_LIMIT') else 10
    )
    
    return posts