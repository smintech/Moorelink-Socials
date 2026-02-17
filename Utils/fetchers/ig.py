import asyncio
import json
import random
import time
import re
from typing import Dict, Optional, Any, Tuple, List, Set
from playwright.async_api import async_playwright, Page, BrowserContext
import logging
from urllib.parse import quote
import os
from Utils import config

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

async def _log_current_url(page: Page, context_msg: str = ""):
    """Log current URL with context"""
    current_url = page.url
    logger.info(f"📍 URL {context_msg}: {current_url}", indent=1)
    return current_url

class InstagramScraper:
    def __init__(self, cookies: List[Dict], logger: DetailedLogger, max_concurrent: int = 3):
        self.cookies = cookies
        self.logger = logger
        self.max_concurrent = max_concurrent
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.intercepted_videos = {}
        self.csrf_token = None
        self.ig_app_id = "936619743392459"
        self.post_doc_id = "8845758582119845"
        self.semaphore = asyncio.Semaphore(max_concurrent)

        for cookie in cookies:
            if cookie['name'] == 'csrftoken':
                self.csrf_token = cookie['value']
                break

    def get_random_delay(self, min_sec=0.1, max_sec=0.3) -> float:
        """OPTIMIZED: Minimal random delays"""
        return random.uniform(min_sec, max_sec)

    async def dismiss_popups(self, page: Page):
        """Click popup buttons quickly without waiting"""
        try:
            selectors = [
                'button:has-text("Not now")',
                'button:has-text("Allow all cookies")',
            ]
            for selector in selectors:
                try:
                    elem = await page.locator(selector).first
                    if elem and await elem.is_visible(timeout=500):
                        await elem.click()
                except:
                    pass
        except:
            pass

    async def intercept_video_urls(self, page: Page):
        """Intercept video URLs from network"""
        async def handle_route(route):
            url = route.request.url
            if any(x in url for x in ['.mp4', '.m3u8', 'video', 'media']):
                if not url.startswith('blob'):
                    self.intercepted_videos[id(page)] = url
            try:
                await route.continue_()
            except:
                pass
        
        await page.route("**/*", handle_route)

    async def get_headers(self) -> Dict[str, str]:
        """Get GraphQL headers"""
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
        """STRATEGY 1: GraphQL API (Most reliable)"""
        try:
            headers = await self.get_headers()
            
            variables = quote(json.dumps({
                'shortcode': shortcode,
                'fetch_tagged_user_count': None,
                'hoisted_comment_id': None,
                'hoisted_reply_id': None
            }, separators=(',', ':')))
            
            body = f"variables={variables}&doc_id={self.post_doc_id}"
            
            script = f"""
                async () => {{
                    try {{
                        const response = await fetch('https://www.instagram.com/graphql/query', {{
                            method: 'POST',
                            headers: {json.dumps(headers)},
                            body: '{body}',
                            credentials: 'include'
                        }});
                        const data = await response.json();
                        return data;
                    }} catch (e) {{
                        return {{error: e.message}};
                    }}
                }}
            """
            
            result = await asyncio.wait_for(page.evaluate(script), timeout=10)
            
            if result and 'data' in result and 'xdt_shortcode_media' in result['data']:
                media = result['data']['xdt_shortcode_media']
                
                caption_edges = media.get('edge_media_to_caption', {}).get('edges', [])
                if caption_edges and len(caption_edges) > 0:
                    caption_text = caption_edges[0]['node'].get('text', '')
                    if caption_text:
                        return caption_text
                
                alt_text = media.get('accessibility_caption', '')
                if alt_text:
                    return alt_text
            
            return None
        except Exception as e:
            self.logger.debug(f"GraphQL error: {str(e)[:50]}", indent=3)
            return None

    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        """STRATEGY 2: DOM extraction with multiple fallbacks"""
        script = r"""
            () => {
                const strategies = [];
                
                // Strategy A: Instagram's caption classes
                let captionDiv = document.querySelector('div._aacl._a9zr._a9zo._a9z9, div._aacl._a9zr');
                if (captionDiv) {
                    const text = captionDiv.innerText?.trim();
                    if (text && text.length > 5) {
                        strategies.push({text: text, source: '_aacl'});
                    }
                }
                
                // Strategy B: h1 elements
                const h1Elements = document.querySelectorAll('h1');
                for (let h1 of h1Elements) {
                    const text = h1.innerText?.trim();
                    if (text && text.length > 20 && text.length < 5000) {
                        const parent = h1.closest('article, div[role="dialog"], main');
                        if (parent) {
                            strategies.push({text: text, source: 'h1'});
                            break;
                        }
                    }
                }
                
                // Strategy C: span elements
                const spans = document.querySelectorAll('span[class*="caption"], span._aacl, div[role="dialog"] span');
                for (let span of spans) {
                    const text = span.innerText?.trim();
                    if (text && text.length > 30 && text.length < 5000) {
                        strategies.push({text: text, source: 'span'});
                        break;
                    }
                }
                
                // Strategy D: Meta description
                const metaDesc = document.querySelector('meta[name="description"], meta[property="og:description"]');
                if (metaDesc) {
                    const content = metaDesc.getAttribute('content');
                    if (content && content.length > 20) {
                        const cleaned = content.replace(/^[^,]+,\s*/, '').trim();
                        if (cleaned.length > 20) {
                            strategies.push({text: cleaned, source: 'meta'});
                        }
                    }
                }
                
                if (strategies.length > 0) {
                    strategies.sort((a, b) => b.text.length - a.text.length);
                    return strategies[0];
                }
                
                return null;
            }
        """
        
        try:
            result = await asyncio.wait_for(page.evaluate(script), timeout=8)
            if result and result['text']:
                caption = result['text']
                caption = re.sub(r'\s*more\s*$', '', caption, flags=re.IGNORECASE).strip()
                return caption
        except Exception as e:
            self.logger.debug(f"DOM error: {str(e)[:50]}", indent=3)
        
        return None

    async def extract_caption_from_shared_data(self, page: Page) -> Optional[str]:
        """STRATEGY 3: SharedData extraction"""
        try:
            script = r"""
                () => {
                    if (window._sharedData && window._sharedData.entry_data) {
                        const postPage = window._sharedData.entry_data.PostPage;
                        if (postPage && postPage[0] && postPage[0].graphql) {
                            const media = postPage[0].graphql.shortcode_media;
                            if (media && media.edge_media_to_caption && media.edge_media_to_caption.edges.length > 0) {
                                return media.edge_media_to_caption.edges[0].node.text;
                            }
                        }
                    }
                    return null;
                }
            """
            
            result = await asyncio.wait_for(page.evaluate(script), timeout=5)
            if result:
                return result
        except Exception as e:
            self.logger.debug(f"SharedData error: {str(e)[:50]}", indent=3)
        
        return None

    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
        """Master caption extraction: all strategies with timeout"""
        caption = ""
        
        await asyncio.sleep(0.3)  # Minimal wait for DOM rendering
        
        # Parallel extraction attempts with timeout
        try:
            tasks = []
            
            # Strategy 1: GraphQL (if shortcode available)
            if shortcode:
                tasks.append(self.extract_caption_graphql(page, shortcode))
            
            # Strategy 2: DOM
            tasks.append(self.extract_caption_from_dom(page))
            
            # Strategy 3: SharedData
            tasks.append(self.extract_caption_from_shared_data(page))
            
            # Race - return first successful result
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, str) and result:
                        return result
                    elif result and not isinstance(result, Exception):
                        return str(result)
        except Exception as e:
            self.logger.debug(f"Caption extraction timeout: {str(e)[:40]}", indent=2)
        
        return caption

    async def extract_media_from_post(self, page: Page, post_url: str) -> Tuple[str, bool]:
        """Extract media URL (image or video) - optimized"""
        is_video = '/reel/' in post_url or '/tv/' in post_url
        
        if is_video:
            # Check intercepted videos first
            if self.intercepted_videos:
                for video_url in self.intercepted_videos.values():
                    if video_url and '.mp4' in video_url:
                        self.intercepted_videos.clear()
                        return video_url, True
            
            script = r"""
                () => {
                    let video = document.querySelector('video[src]');
                    if (video?.src && !video.src.startsWith('blob')) return video.src;
                    
                    let source = document.querySelector('video source[src]');
                    if (source?.src && !source.src.startsWith('blob')) return source.src;
                    
                    video = document.querySelector('video[data-video-url]');
                    if (video) return video.getAttribute('data-video-url');
                    
                    return '';
                }
            """
            
            try:
                media_url = await asyncio.wait_for(page.evaluate(script), timeout=5)
                if media_url and not media_url.startswith('blob'):
                    return media_url, True
            except Exception as e:
                self.logger.debug(f"Video extraction error: {str(e)[:40]}", indent=3)
        
        else:
            script = r"""
                () => {
                    const metaImg = document.querySelector('meta[property="og:image"]');
                    if (metaImg) {
                        const content = metaImg.getAttribute('content');
                        if (content && content.includes('cdninstagram')) return content;
                    }
                    
                    const imgs = document.querySelectorAll('img[src]');
                    const candidates = [];
                    
                    for (const img of imgs) {
                        const src = img.src;
                        if ((src.includes('cdninstagram') || src.includes('fbcdn')) && img.naturalWidth > 400) {
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
                media_url = await asyncio.wait_for(page.evaluate(script), timeout=5)
                if media_url and not media_url.startswith('blob'):
                    return media_url, False
            except Exception as e:
                self.logger.debug(f"Image extraction error: {str(e)[:40]}", indent=3)
        
        return "", is_video

    async def scrape_post(self, page: Page, post_url: str, shortcode: str, post_index: int) -> Optional[Dict]:
        """Scrape a single post with concurrency control"""
        async with self.semaphore:
            try:
                self.logger.info(f"[{post_index}] POST: {shortcode}", indent=1)
                
                # Navigate to post with optimized timeout
                await asyncio.wait_for(
                    page.goto(post_url, wait_until='domcontentloaded', timeout=20000),
                    timeout=22
                )
                
                # Non-blocking network wait
                try:
                    await asyncio.wait_for(
                        page.wait_for_load_state('networkidle'),
                        timeout=8
                    )
                except asyncio.TimeoutError:
                    pass  # Continue anyway
                
                await asyncio.sleep(0.2)
                
                # Parallel extraction
                caption_task = self.extract_caption_from_post(page, shortcode)
                media_task = self.extract_media_from_post(page, post_url)
                
                caption, (media_url, is_video) = await asyncio.gather(
                    caption_task,
                    media_task
                )
                
                if media_url and not media_url.startswith('blob'):
                    post_data = {
                        "url": post_url,
                        "caption": caption,
                        "media_url": media_url,
                        "is_video": is_video,
                    }
                    self.logger.success(f"{'🎥 VIDEO' if is_video else '📸 IMAGE'} | Caption: {len(caption)} chars", indent=2)
                    return post_data
                else:
                    self.logger.warning("No media URL found", indent=2)
                    return None
                
            except Exception as e:
                self.logger.error(f"Post error: {str(e)[:50]}", indent=2)
                return None

    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        self.logger.step("Initialize Scraper", f"Target: @{username} | Limit: {post_limit} | Concurrent: {self.max_concurrent}")
        
        async with async_playwright() as p:
            self.logger.info("Launching browser...", indent=1)
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--no-sandbox',
                    '--window-size=1920,1080'
                ]
            )
            self.logger.success("Browser launched", indent=1)
            
            context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080}
            )
            
            await context.add_cookies(self.cookies)
            self.logger.success(f"Cookies loaded: {len(self.cookies)}", indent=1)
            
            try:
                # === PHASE 1: LOAD PROFILE ===
                self.logger.step("Load Profile", f"https://www.instagram.com/{username}/")
                
                profile_page = await context.new_page()
                profile_page.set_default_navigation_timeout(40000)
                profile_page.set_default_timeout(20000)
                
                await self.intercept_video_urls(profile_page)
                
                profile_url = f"https://www.instagram.com/{username}/"
                await profile_page.goto(profile_url, wait_until='domcontentloaded', timeout=25000)
                
                if 'accounts/login' in profile_page.url:
                    self.logger.error("Redirected to login - cookies expired")
                    return []
                
                await self.dismiss_popups(profile_page)
                
                # Non-blocking network wait
                try:
                    await asyncio.wait_for(
                        profile_page.wait_for_load_state('networkidle'),
                        timeout=10
                    )
                except asyncio.TimeoutError:
                    pass
                
                # === PHASE 2: LOAD POSTS ===
                self.logger.step("Scroll & Discover Posts", "Loading post links...")
                
                last_height = 0
                no_change_count = 0
                
                for scroll_iter in range(8):  # Max 8 scrolls
                    links = await profile_page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
                    self.logger.debug(f"Posts found: {len(links)} (scroll {scroll_iter + 1})", indent=2)
                    
                    if len(links) >= post_limit:
                        break
                    
                    await profile_page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(0.4)  # Minimal scroll delay
                    
                    new_height = await profile_page.evaluate('document.body.scrollHeight')
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 2:
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height
                
                # === PHASE 3: EXTRACT POST URLS ===
                self.logger.step("Extract Post URLs", "Gathering unique post links...")
                
                script = r"""
                    () => {
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        return [...new Set(
                            links
                                .filter(a => a.href.includes('/p/') || a.href.includes('/reel/') || a.href.includes('/tv/'))
                                .map(a => a.href.split('?')[0])
                        )];
                    }
                """
                
                post_urls = await profile_page.evaluate(script)
                post_urls = post_urls[:post_limit]
                
                self.logger.success(f"Found {len(post_urls)} unique posts", indent=1)
                await profile_page.close()
                
                # === PHASE 4: SCRAPE POSTS CONCURRENTLY ===
                self.logger.step("Scrape Posts Concurrently", f"Processing {len(post_urls)} posts with {self.max_concurrent} workers")
                
                # Create page pool
                pages = []
                for i in range(self.max_concurrent):
                    page = await context.new_page()
                    page.set_default_navigation_timeout(35000)
                    page.set_default_timeout(18000)
                    await self.intercept_video_urls(page)
                    pages.append(page)
                
                # Distribute posts across pages
                tasks = []
                for idx, post_url in enumerate(post_urls):
                    try:
                        shortcode = post_url.split('/p/')[-1].split('/')[0]
                        if not shortcode:
                            shortcode = post_url.split('/reel/')[-1].split('/')[0]
                    except:
                        shortcode = f"post_{idx}"
                    
                    page = pages[idx % self.max_concurrent]
                    tasks.append(self.scrape_post(page, post_url, shortcode, idx + 1))
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                posts = [r for r in results if r and not isinstance(r, Exception)]
                
                # Close all pages
                for page in pages:
                    await page.close()
                
                self.logger.step("Scraping Complete", f"✅ Successfully scraped {len(posts)}/{len(post_urls)} posts")
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
    """Fetch Instagram posts - OPTIMIZED for speed and concurrency
    
    Args:
        account: Instagram username (with or without @)
        cookies: List of cookie dicts. If None, loads from IG_COOKIES env var
        max_concurrent: Number of concurrent post scraping tasks (default: 3)
    
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
    
    scraper = InstagramScraper(cookies=cookies, logger=logger, max_concurrent=max_concurrent)
    posts = await scraper.scrape_profile(username=account, post_limit=config.POST_LIMIT)
    
    return posts