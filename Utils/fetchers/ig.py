import asyncio
import json
import random
import time
import re
from typing import Dict, Optional, Any, Tuple, List
from playwright.async_api import async_playwright, Page
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
            format='[%(asctime)s.%(msecs)03d] %(levelname)-8s | %(funcName)-20s | %(message)s',
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
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.csrf_token = None
        self.ig_app_id = "936619743392459"
        self.post_doc_id = "8845758582119845"

        for cookie in cookies:
            if cookie['name'] == 'csrftoken':
                self.csrf_token = cookie['value']
                break

    def get_random_delay(self, min_sec=0.1, max_sec=0.5) -> float:
        """OPTIMIZED: Reduced default delays"""
        delay = random.uniform(min_sec, max_sec)
        return delay

    async def dismiss_popups(self, page: Page):
        """Click popup buttons - optimized"""
        try:
            selectors = [
                'button:has-text("Not now")',
                'button:has-text("Allow all cookies")',
            ]
            for selector in selectors:
                try:
                    elem = await page.locator(selector).first
                    if elem and await elem.is_visible(timeout=1000):
                        await elem.click()
                        self.logger.success(f"Dismissed: {selector}", indent=2)
                        await asyncio.sleep(0.2)
                except:
                    pass
        except:
            pass

    async def get_headers(self) -> Dict[str, str]:
        """Get headers for GraphQL requests"""
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.instagram.com',
            'referer': 'https://www.instagram.com/',
            'user-agent': self.user_agents[0],
            'x-csrftoken': self.csrf_token or '',
            'x-ig-app-id': self.ig_app_id,
            'x-requested-with': 'XMLHttpRequest',
        }
        return headers

    async def extract_caption_fast(self, page: Page, shortcode: str = "") -> str:
        """OPTIMIZED: Faster caption extraction with priority strategies"""
        try:
            # Strategy 1: Try GraphQL (fastest for reliable extraction)
            if shortcode:
                try:
                    headers = await self.get_headers()
                    variables = quote(json.dumps({
                        'shortcode': shortcode,
                        'fetch_tagged_user_count': None,
                    }, separators=(',', ':')))
                    body = f"variables={variables}&doc_id={self.post_doc_id}"
                    
                    script = f"""
                        (async () => {{
                            try {{
                                const response = await fetch('https://www.instagram.com/graphql/query', {{
                                    method: 'POST',
                                    headers: {json.dumps(headers)},
                                    body: '{body}',
                                    credentials: 'include'
                                }});
                                const data = await response.json();
                                if (data.data?.xdt_shortcode_media?.edge_media_to_caption?.edges?.[0]) {{
                                    return data.data.xdt_shortcode_media.edge_media_to_caption.edges[0].node.text;
                                }}
                                return null;
                            }} catch (e) {{
                                return null;
                            }}
                        }})()
                    """
                    caption = await page.evaluate(script)
                    if caption:
                        return caption
                except:
                    pass

            # Strategy 2: Fast DOM extraction
            script = r"""
                () => {
                    // Quick check for caption div
                    let captionDiv = document.querySelector('div._aacl._a9zr');
                    if (captionDiv?.innerText?.trim()) return captionDiv.innerText.trim();
                    
                    // Check meta description
                    const metaDesc = document.querySelector('meta[property="og:description"]');
                    if (metaDesc) {
                        const content = metaDesc.getAttribute('content');
                        if (content?.length > 20) return content.replace(/^[^,]+,\s*/, '').trim();
                    }
                    
                    return null;
                }
            """
            caption = await page.evaluate(script)
            if caption:
                return caption
                
        except:
            pass
        
        return ""

    async def extract_media_fast(self, page: Page, post_url: str) -> Tuple[str, bool]:
        """OPTIMIZED: Faster media extraction"""
        is_video = '/reel/' in post_url or '/tv/' in post_url

        script = r"""
            () => {
                if (""" + str(is_video).lower() + r""") {
                    // Video extraction
                    let video = document.querySelector('video[src]');
                    if (video?.src && !video.src.startsWith('blob')) return video.src;
                    
                    let source = document.querySelector('video source[src]');
                    if (source?.src && !source.src.startsWith('blob')) return source.src;
                } else {
                    // Image extraction
                    const metaImg = document.querySelector('meta[property="og:image"]');
                    if (metaImg) {
                        const content = metaImg.getAttribute('content');
                        if (content?.includes('cdninstagram')) return content;
                    }
                    
                    const imgs = document.querySelectorAll('img[src]');
                    for (const img of imgs) {
                        if ((img.src.includes('cdninstagram') || img.src.includes('fbcdn')) && img.naturalWidth > 400) {
                            return img.src;
                        }
                    }
                }
                return null;
            }
        """

        try:
            media_url = await page.evaluate(script)
            if media_url and not media_url.startswith('blob'):
                return media_url, is_video
        except:
            pass

        return "", is_video

    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        self.logger.step("Start Scraping", f"Target: @{username} | Limit: {post_limit}")
        
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
            self.logger.success(f"Context ready with {len(self.cookies)} cookies", indent=1)
            
            page = await context.new_page()
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(15000)
            
            try:
                profile_url = f"https://www.instagram.com/{username}/"
                self.logger.step("Load Profile", profile_url)
                
                # OPTIMIZED: Use domcontentloaded instead of networkidle
                await page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
                await _log_current_url(page, "[PROFILE_LOADED]")
                
                if 'accounts/login' in page.url:
                    self.logger.error("Redirected to login - cookies expired")
                    return []
                
                await self.dismiss_popups(page)
                await asyncio.sleep(0.5)  # OPTIMIZED: Reduced from 3s
                
                # OPTIMIZED: Faster post collection with shorter scroll loop
                self.logger.info("Collecting post URLs...", indent=1)
                post_urls = []
                last_height = 0
                no_change_count = 0
                
                for i in range(15):  # OPTIMIZED: Reduced from 30
                    script = r"""
                        () => Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'))
                            .map(a => a.href.split('?')[0])
                            .filter((url, idx, arr) => arr.indexOf(url) === idx)
                    """
                    post_urls = await page.evaluate(script)
                    
                    self.logger.debug(f"Found {len(post_urls)} posts after scroll {i+1}", indent=2)
                    if len(post_urls) >= post_limit:
                        self.logger.success(f"Sufficient posts found: {len(post_urls)}", indent=2)
                        break
                    
                    await page.evaluate('window.scrollBy(0, 500)')  # Smaller scroll increments
                    await asyncio.sleep(0.3)  # OPTIMIZED: Reduced from 2-3s
                    
                    new_height = await page.evaluate('document.body.scrollHeight')
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 2:  # OPTIMIZED: Reduced threshold
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height
                
                # OPTIMIZED: Process posts in parallel where possible
                self.logger.step("Extracting Posts", f"Processing {min(len(post_urls), post_limit)} posts")
                
                posts = []
                post_urls = post_urls[:post_limit]
                
                for idx, post_url in enumerate(post_urls):
                    self.logger.info(f"[{idx+1}/{len(post_urls)}] {post_url.split('/')[-2]}", indent=1)
                    
                    shortcode = ""
                    try:
                        shortcode = post_url.split('/p/')[-1].split('/')[0] or post_url.split('/reel/')[-1].split('/')[0]
                    except:
                        pass
                    
                    try:
                        # OPTIMIZED: domcontentloaded instead of networkidle
                        await page.goto(post_url, wait_until='domcontentloaded', timeout=15000)
                        await asyncio.sleep(0.5)  # OPTIMIZED: Reduced from 2s
                        
                        # Extract caption and media concurrently
                        caption_task = asyncio.create_task(self.extract_caption_fast(page, shortcode))
                        media_task = asyncio.create_task(self.extract_media_fast(page, post_url))
                        
                        caption, (media_url, is_video) = await asyncio.gather(caption_task, media_task)
                        
                        if media_url and not media_url.startswith('blob'):
                            posts.append({
                                "url": post_url,
                                "caption": caption,
                                "media_url": media_url,
                                "is_video": is_video,
                            })
                            self.logger.success(f"Saved ({'VIDEO 🎥' if is_video else 'IMAGE 📸'}, caption: {len(caption)} chars)", indent=2)
                        else:
                            self.logger.warning("No valid media URL", indent=2)
                        
                        await asyncio.sleep(self.get_random_delay(0.1, 0.3))  # OPTIMIZED: Reduced
                        
                    except Exception as e:
                        self.logger.error(f"Error: {str(e)[:50]}", indent=2)
                        try:
                            await page.goto(profile_url, wait_until='domcontentloaded', timeout=15000)
                        except:
                            pass
                
                self.logger.success(f"Complete: {len(posts)} posts extracted", indent=1)
                return posts
                
            except Exception as e:
                self.logger.error(f"Fatal error: {e}", indent=1)
                return []
            
            finally:
                await browser.close()
                self.logger.success("Browser closed", indent=1)


async def fetch_ig_urls(account: str, cookies: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Fetch Instagram posts - OPTIMIZED for speed"""
    account = account.lstrip('@')
    
    logger.step("Configuration", "Setting up scraper")
    
    if cookies is None:
        cookies_str = os.getenv('IG_COOKIES')
        if not cookies_str:
            logger.error("No IG_COOKIES environment variable set")
            return []
        try:
            cookies = json.loads(cookies_str)
            logger.success(f"Loaded {len(cookies)} cookies")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid IG_COOKIES JSON: {str(e)}")
            return []
    
    scraper = InstagramScraper(cookies=cookies, logger=logger)
    posts = await scraper.scrape_profile(username=account, post_limit=config.POST_LIMIT)
    
    return posts