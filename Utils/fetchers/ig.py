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
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.intercepted_videos = {}
        self.csrf_token = None
        self.ig_app_id = "936619743392459"
        self.post_doc_id = "8845758582119845"

        for cookie in cookies:
            if cookie['name'] == 'csrftoken':
                self.csrf_token = cookie['value']
                break

    def get_random_delay(self, min_sec=0.5, max_sec=1.5) -> float:
        """OPTIMIZED: Reasonable delays to balance speed vs stability"""
        return random.uniform(min_sec, max_sec)

    async def dismiss_popups(self, page: Page):
        """Click popup buttons quickly"""
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
                        self.logger.success(f"Dismissed popup", indent=2)
                        await asyncio.sleep(0.2)
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
                    self.intercepted_videos[len(self.intercepted_videos)] = url
            try:
                await route.continue_()
            except:
                pass
        
        await page.route("**/*", handle_route)
        self.logger.success("Video interception enabled", indent=1)

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
            self.logger.info("GraphQL caption extraction...", indent=2)
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
            
            result = await page.evaluate(script)
            
            if result and 'data' in result and 'xdt_shortcode_media' in result['data']:
                media = result['data']['xdt_shortcode_media']
                
                caption_edges = media.get('edge_media_to_caption', {}).get('edges', [])
                if caption_edges and len(caption_edges) > 0:
                    caption_text = caption_edges[0]['node'].get('text', '')
                    if caption_text:
                        self.logger.success(f"GraphQL caption: {len(caption_text)} chars", indent=3)
                        return caption_text
                
                alt_text = media.get('accessibility_caption', '')
                if alt_text:
                    self.logger.success(f"GraphQL alt text: {len(alt_text)} chars", indent=3)
                    return alt_text
            
            return None
        except Exception as e:
            self.logger.debug(f"GraphQL error: {str(e)[:60]}", indent=3)
            return None

    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        """STRATEGY 2: DOM extraction with multiple fallbacks"""
        self.logger.info("DOM caption extraction...", indent=2)
        
        script = r"""
            () => {
                const strategies = [];
                
                // Strategy A: Instagram's caption classes
                let captionDiv = document.querySelector('div._aacl._a9zr._a9zo._a9z9, div._aacl._a9zr');
                if (captionDiv) {
                    const text = captionDiv.innerText?.trim();
                    if (text && text.length > 5) {
                        strategies.push({text: text, source: '_aacl caption div'});
                    }
                }
                
                // Strategy B: h1 elements
                const h1Elements = document.querySelectorAll('h1');
                for (let h1 of h1Elements) {
                    const text = h1.innerText?.trim();
                    if (text && text.length > 20 && text.length < 5000) {
                        const parent = h1.closest('article, div[role="dialog"], main');
                        if (parent) {
                            strategies.push({text: text, source: 'h1 in content area'});
                            break;
                        }
                    }
                }
                
                // Strategy C: span elements
                const spans = document.querySelectorAll('span[class*="caption"], span._aacl, div[role="dialog"] span');
                for (let span of spans) {
                    const text = span.innerText?.trim();
                    if (text && text.length > 30 && text.length < 5000) {
                        strategies.push({text: text, source: 'span caption class'});
                        break;
                    }
                }
                
                // Strategy D: Tree walker text nodes
                const containers = [
                    document.querySelector('article'),
                    document.querySelector('div[role="dialog"]'),
                    document.querySelector('main section')
                ];
                
                for (let container of containers) {
                    if (container) {
                        const walker = document.createTreeWalker(
                            container,
                            NodeFilter.SHOW_TEXT,
                            null,
                            false
                        );
                        
                        let node;
                        const texts = [];
                        while (node = walker.nextNode()) {
                            const text = node.textContent?.trim();
                            if (text && text.length > 40 && text.length < 2000 && !text.includes('http')) {
                                texts.push(text);
                            }
                        }
                        
                        if (texts.length > 0) {
                            const longest = texts.reduce((a, b) => a.length > b.length ? a : b);
                            strategies.push({text: longest, source: 'treeWalker text nodes'});
                            break;
                        }
                    }
                }
                
                // Strategy E: Meta description
                const metaDesc = document.querySelector('meta[name="description"], meta[property="og:description"]');
                if (metaDesc) {
                    const content = metaDesc.getAttribute('content');
                    if (content && content.length > 20) {
                        const cleaned = content.replace(/^[^,]+,\s*/, '').trim();
                        if (cleaned.length > 20) {
                            strategies.push({text: cleaned, source: 'meta description'});
                        }
                    }
                }
                
                // Strategy F: JSON-LD
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (let script of scripts) {
                    try {
                        const data = JSON.parse(script.innerText);
                        if (data.caption || data.description) {
                            const text = data.caption || data.description;
                            if (text.length > 10) {
                                strategies.push({text: text, source: 'JSON-LD'});
                            }
                        }
                    } catch (e) {}
                }
                
                if (strategies.length > 0) {
                    const priorityOrder = ['_aacl', 'h1', 'span', 'treeWalker', 'JSON-LD', 'meta'];
                    strategies.sort((a, b) => {
                        const aPriority = priorityOrder.findIndex(p => a.source.includes(p));
                        const bPriority = priorityOrder.findIndex(p => b.source.includes(p));
                        if (aPriority !== bPriority) return aPriority - bPriority;
                        return b.text.length - a.text.length;
                    });
                    return strategies[0];
                }
                
                return null;
            }
        """
        
        try:
            result = await page.evaluate(script)
            if result and result['text']:
                caption = result['text']
                caption = re.sub(r'\s*more\s*$', '', caption, flags=re.IGNORECASE).strip()
                self.logger.success(f"DOM caption via {result['source']}: {len(caption)} chars", indent=3)
                return caption
        except Exception as e:
            self.logger.debug(f"DOM error: {str(e)[:60]}", indent=3)
        
        return None

    async def extract_caption_from_shared_data(self, page: Page) -> Optional[str]:
        """STRATEGY 3: SharedData extraction"""
        try:
            self.logger.info("SharedData extraction...", indent=2)
            
            script = r"""
                () => {
                    // Try _sharedData
                    if (window._sharedData && window._sharedData.entry_data) {
                        const postPage = window._sharedData.entry_data.PostPage;
                        if (postPage && postPage[0] && postPage[0].graphql) {
                            const media = postPage[0].graphql.shortcode_media;
                            if (media && media.edge_media_to_caption && media.edge_media_to_caption.edges.length > 0) {
                                return {
                                    text: media.edge_media_to_caption.edges[0].node.text,
                                    source: '_sharedData'
                                };
                            }
                        }
                    }
                    
                    // Try __additionalDataLoaded
                    if (window.__additionalDataLoaded) {
                        for (let key in window.__additionalDataLoaded) {
                            if (key.includes('/p/') || key.includes('/reel/')) {
                                const data = window.__additionalDataLoaded[key];
                                if (data && data.graphql && data.graphql.shortcode_media) {
                                    const media = data.graphql.shortcode_media;
                                    if (media.edge_media_to_caption && media.edge_media_to_caption.edges.length > 0) {
                                        return {
                                            text: media.edge_media_to_caption.edges[0].node.text,
                                            source: '__additionalDataLoaded'
                                        };
                                    }
                                }
                            }
                        }
                    }
                    
                    return null;
                }
            """
            
            result = await page.evaluate(script)
            if result and result['text']:
                self.logger.success(f"SharedData caption: {len(result['text'])} chars", indent=3)
                return result['text']
        except Exception as e:
            self.logger.debug(f"SharedData error: {str(e)[:60]}", indent=3)
        
        return None

    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
        """Master caption extraction: all strategies"""
        caption = ""
        
        await asyncio.sleep(1.5)  # OPTIMIZED: Reduced from 2s
        
        # Strategy 1: GraphQL
        if shortcode:
            caption = await self.extract_caption_graphql(page, shortcode) or ""
        
        # Strategy 2: DOM
        if not caption:
            caption = await self.extract_caption_from_dom(page) or ""
        
        # Strategy 3: SharedData
        if not caption:
            caption = await self.extract_caption_from_shared_data(page) or ""
        
        # Strategy 4: Fallback raw text
        if not caption:
            self.logger.info("Raw text fallback...", indent=2)
            try:
                raw_text = await page.evaluate('() => document.body.innerText')
                lines = [l.strip() for l in raw_text.split('\n') if 30 < len(l.strip()) < 1000]
                if lines:
                    filtered = [l for l in lines if not any(x in l.lower() for x in ['followers', 'following', 'posts', 'meta', 'privacy', 'terms'])]
                    if filtered:
                        caption = filtered[0]
                        self.logger.success(f"Raw text fallback: {len(caption)} chars", indent=3)
            except Exception as e:
                self.logger.debug(f"Raw text error: {str(e)[:60]}", indent=3)
        
        if not caption:
            self.logger.warning("All caption strategies failed", indent=2)
        
        return caption

    async def extract_media_from_post(self, page: Page, post_url: str) -> Tuple[str, bool]:
        """Extract media URL (image or video)"""
        is_video = '/reel/' in post_url or '/tv/' in post_url
        await asyncio.sleep(0.5)  # OPTIMIZED: Reduced from 1s
        
        if is_video:
            self.logger.debug("Video extraction...", indent=2)
            
            if self.intercepted_videos:
                for video_url in self.intercepted_videos.values():
                    if video_url and not video_url.startswith('blob') and '.mp4' in video_url:
                        self.logger.success(f"Intercepted video URL", indent=3)
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
                media_url = await page.evaluate(script)
                if media_url and not media_url.startswith('blob'):
                    self.logger.success(f"Video extracted", indent=3)
                    return media_url, True
            except Exception as e:
                self.logger.debug(f"Video extraction error: {str(e)[:40]}", indent=3)
        
        else:
            self.logger.debug("Image extraction...", indent=2)
            
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
                media_url = await page.evaluate(script)
                if media_url and not media_url.startswith('blob'):
                    self.logger.success(f"Image extracted", indent=3)
                    return media_url, False
            except Exception as e:
                self.logger.debug(f"Image extraction error: {str(e)[:40]}", indent=3)
        
        self.logger.warning("No media URL found", indent=2)
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
            self.logger.success(f"Added {len(self.cookies)} cookies", indent=1)
            
            page = await context.new_page()
            page.set_default_navigation_timeout(60000)  # IMPORTANT: Keep at 60s for stability
            page.set_default_timeout(30000)
            
            await self.intercept_video_urls(page)
            
            try:
                profile_url = f"https://www.instagram.com/{username}/"
                self.logger.step("Load Profile", profile_url)
                
                await page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
                await _log_current_url(page, "[PROFILE_LOADED]")
                await asyncio.sleep(1)  # OPTIMIZED: Reduced from 3s
                
                if 'accounts/login' in page.url:
                    self.logger.error("Redirected to login - cookies expired")
                    return []
                
                await self.dismiss_popups(page)
                
                # IMPORTANT: Non-blocking networkidle wait
                try:
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    self.logger.success("Page loaded to networkidle", indent=1)
                except:
                    self.logger.warning("Network timeout, continuing", indent=1)
                
                # Scroll to load posts
                self.logger.info("Scrolling to load posts...", indent=1)
                last_height = 0
                no_change_count = 0
                
                for i in range(12):  # OPTIMIZED: Reduced from 15-30
                    links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
                    self.logger.debug(f"Found {len(links)} posts after scroll {i+1}", indent=2)
                    if len(links) >= post_limit:
                        self.logger.success(f"Found {len(links)} posts", indent=2)
                        break
                    
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(0.8)  # OPTIMIZED: Reduced from 2-3s
                    
                    new_height = await page.evaluate('document.body.scrollHeight')
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 3:
                            self.logger.warning("No more posts loading", indent=2)
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height
                
                # Extract posts
                self.logger.step("Extracting Posts", f"Processing up to {post_limit} posts")
                
                posts = []
                processed_urls = set()
                
                script = r"""
                    () => {
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        return links
                            .filter(a => a.href.includes('/p/') || a.href.includes('/reel/') || a.href.includes('/tv/'))
                            .map(a => a.href.split('?')[0])
                            .filter((url, idx, arr) => arr.indexOf(url) === idx);
                    }
                """
                
                post_urls = await page.evaluate(script)
                self.logger.info(f"Found {len(post_urls)} unique post URLs", indent=1)
                
                for idx, post_url in enumerate(post_urls[:post_limit]):
                    if idx >= post_limit:
                        break
                    
                    if post_url in processed_urls:
                        continue
                    
                    processed_urls.add(post_url)
                    self.logger.info(f"\n[{idx+1}/{post_limit}] Processing: {post_url.split('/')[-2]}", indent=1)
                    
                    shortcode = ""
                    try:
                        shortcode = post_url.split('/p/')[-1].split('/')[0]
                        if not shortcode:
                            shortcode = post_url.split('/reel/')[-1].split('/')[0]
                        self.logger.debug(f"Shortcode: {shortcode}", indent=2)
                    except:
                        pass
                    
                    try:
                        self.intercepted_videos.clear()
                        
                        # IMPORTANT: 30s timeout for post navigation
                        await page.goto(post_url, wait_until='domcontentloaded', timeout=30000)
                        await _log_current_url(page, "[POST_LOADED]")
                        await asyncio.sleep(1)  # OPTIMIZED: Reduced from 2s
                        
                        # Non-blocking networkidle
                        try:
                            await page.wait_for_load_state('networkidle', timeout=15000)
                            self.logger.success("Post loaded to networkidle", indent=2)
                        except:
                            self.logger.warning("Post network timeout, continuing", indent=2)
                        
                        # Extract caption and media
                        caption = await self.extract_caption_from_post(page, shortcode)
                        media_url, is_video = await self.extract_media_from_post(page, post_url)
                        
                        if media_url and not media_url.startswith('blob'):
                            posts.append({
                                "url": post_url,
                                "caption": caption,
                                "media_url": media_url,
                                "is_video": is_video,
                            })
                            self.logger.success(f"Saved ({'VIDEO 🎥' if is_video else 'IMAGE 📸'}, caption: {len(caption)} chars)", indent=2)
                        else:
                            self.logger.warning(f"Invalid media URL, skipping", indent=2)
                        
                        self.logger.info("Going back to profile", indent=2)
                        await page.go_back(wait_until='domcontentloaded', timeout=20000)
                        await _log_current_url(page, "[BACK_TO_PROFILE]")
                        await asyncio.sleep(self.get_random_delay(0.5, 1))  # OPTIMIZED: Reduced
                        
                    except Exception as e:
                        self.logger.error(f"Post error: {str(e)[:60]}", indent=2)
                        try:
                            await page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
                            await _log_current_url(page, "[RECOVERY]")
                        except:
                            self.logger.error("Recovery failed", indent=2)
                
                self.logger.success(f"Scraping complete: {len(posts)} posts", indent=1)
                return posts
                
            except Exception as e:
                self.logger.error(f"Fatal error: {e}", indent=1)
                import traceback
                self.logger.debug(traceback.format_exc(), indent=2)
                return []
            
            finally:
                self.logger.info("Closing browser", indent=1)
                await browser.close()
                self.logger.success("Browser closed", indent=1)


async def fetch_ig_urls(account: str, cookies: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Fetch Instagram posts - OPTIMIZED with proven extraction strategies"""
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