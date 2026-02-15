import logging

import requests

from Utils import config
from Utils import persistence

import random
import time
from typing import Dict, Optional, Any, Tuple, Callable, List

from itertools import islice
import logging
import asyncio
import json
import random
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from typing import List, Dict, Any
import datetime

def _random_delay(min_sec: float = 0.5, max_sec: float = 2.0):
    """Generate random delay with jitter"""
    return random.uniform(min_sec, max_sec)

def _get_enhanced_stealth_script() -> str:
    """Generate comprehensive anti-detection JavaScript"""
    
    return """
    // ============================================
    // COMPREHENSIVE STEALTH SCRIPT
    // ============================================
    
    // 1. WebDriver Detection
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    
    // 2. Chrome Detection
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    
    // 3. Permissions API
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    
    // 4. Plugins Array (fake plugins)
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            {
                0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format",
                filename: "internal-pdf-viewer",
                length: 1,
                name: "Chrome PDF Plugin"
            },
            {
                0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format", 
                filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                length: 1,
                name: "Chrome PDF Viewer"
            },
            {
                0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable"},
                1: {type: "application/x-pnacl", suffixes: "", description: "Portable Native Client Executable"},
                description: "Native Client",
                filename: "internal-nacl-plugin",
                length: 2,
                name: "Native Client"
            }
        ]
    });
    
    // 5. Languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    
    // 6. Platform
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Win32',
        configurable: true
    });
    
    // 7. Hardware Concurrency (CPU cores)
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8,
        configurable: true
    });
    
    // 8. Device Memory
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8,
        configurable: true
    });
    
    // 9. User Agent
    Object.defineProperty(navigator, 'userAgent', {
        get: () => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        configurable: true
    });
    
    // 10. Vendor
    Object.defineProperty(navigator, 'vendor', {
        get: () => 'Google Inc.',
        configurable: true
    });
    
    // 11. WebGL Vendor/Renderer
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.apply(this, arguments);
    };
    
    // 12. Battery API
    if (navigator.getBattery) {
        const originalGetBattery = navigator.getBattery;
        navigator.getBattery = function() {
            return originalGetBattery.apply(this, arguments).then(battery => {
                Object.defineProperty(battery, 'charging', { value: true });
                Object.defineProperty(battery, 'chargingTime', { value: 0 });
                Object.defineProperty(battery, 'dischargingTime', { value: Infinity });
                Object.defineProperty(battery, 'level', { value: 1.0 });
                return battery;
            });
        };
    }
    
    // 13. Connection API
    if (navigator.connection) {
        Object.defineProperty(navigator.connection, 'rtt', { value: 100 });
        Object.defineProperty(navigator.connection, 'downlink', { value: 10 });
        Object.defineProperty(navigator.connection, 'effectiveType', { value: '4g' });
    }
    
    // 14. Screen properties
    Object.defineProperty(screen, 'availWidth', { value: 1920 });
    Object.defineProperty(screen, 'availHeight', { value: 1040 });
    Object.defineProperty(screen, 'width', { value: 1920 });
    Object.defineProperty(screen, 'height', { value: 1080 });
    Object.defineProperty(screen, 'colorDepth', { value: 24 });
    Object.defineProperty(screen, 'pixelDepth', { value: 24 });
    
    // 15. Date object (timezone consistency)
    Date.prototype.getTimezoneOffset = function() {
        return 300; // EST timezone
    };
    
    // 16. Canvas Fingerprinting Protection
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {
        if (type === 'image/png' && this.width === 16 && this.height === 16) {
            // Likely a fingerprinting attempt
            return originalToDataURL.apply(this, arguments);
        }
        return originalToDataURL.apply(this, arguments);
    };
    
    // 17. Media Devices
    if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
        const originalEnumerateDevices = navigator.mediaDevices.enumerateDevices;
        navigator.mediaDevices.enumerateDevices = function() {
            return originalEnumerateDevices.apply(this, arguments).then(devices => {
                return devices.map(device => ({
                    deviceId: device.deviceId,
                    kind: device.kind,
                    label: device.label,
                    groupId: device.groupId,
                    toJSON: function() { return this; }
                }));
            });
        };
    }
    
    // 18. Remove automation indicators
    delete navigator.__proto__.webdriver;
    
    // 19. Iframe detection evasion
    Object.defineProperty(window, 'outerWidth', { value: window.innerWidth });
    Object.defineProperty(window, 'outerHeight', { value: window.innerHeight + 85 });
    
    // 20. Mouse/Touch events (make them look real)
    const originalAddEventListener = EventTarget.prototype.addEventListener;
    EventTarget.prototype.addEventListener = function(type, listener, options) {
        if (type === 'mouseenter' || type === 'mouseleave') {
            // Instagram checks for these
            return originalAddEventListener.apply(this, arguments);
        }
        return originalAddEventListener.apply(this, arguments);
    };
    
    console.log('🛡️ Stealth mode activated');
    """

def _get_random_viewport() -> Dict[str, int]:
    """Get randomized viewport size"""
    viewports = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1536, 'height': 864},
        {'width': 1440, 'height': 900},
        {'width': 1600, 'height': 900},
    ]
    return random.choice(viewports)

def _get_realistic_user_agent() -> str:
    """Get a realistic, recent user agent"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
    ]
    return random.choice(user_agents)

async def _random_scroll(page):
    """Perform random scrolling to mimic human behavior"""
    try:
        scroll_amount = random.randint(100, 500)
        await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
        await asyncio.sleep(_random_delay(0.3, 0.8))
        await page.evaluate(f'window.scrollBy(0, -{scroll_amount // 2})')
    except:
        pass

async def _human_like_type(page, selector: str, text: str):
    """Type text with human-like random delays"""
    await page.click(selector)
    await asyncio.sleep(_random_delay(0.1, 0.3))
    
    for char in text:
        await page.keyboard.type(char)
        await asyncio.sleep(_random_delay(0.05, 0.15))
    
    await asyncio.sleep(_random_delay(0.3, 0.6))

async def _handle_popups(page):
    """Handle various popups that Instagram shows, including anonymous browsing popups"""
    logger = logging.getLogger(__name__)
    logger.info("Checking for popups...")
    popup_handled = False
    
    # Cookie consent popup
    cookie_selectors = [
        ('button:has-text("Allow all cookies")', "Allow all cookies"),
        ('button:has-text("Accept all")', "Accept all"),
        ('button >> text="Allow all cookies"', "Allow all cookies (text)"),
    ]
    
    for selector, description in cookie_selectors:
        try:
            if await page.locator(selector).is_visible(timeout=1000):
                logger.info(f"Found cookie consent: {description}")
                await asyncio.sleep(_random_delay(0.5, 1.0))
                await page.click(selector)
                logger.info(f"Clicked '{description}'")
                await asyncio.sleep(_random_delay(2.0, 3.0))
                popup_handled = True
                break
        except:
            pass
    
    # Decline optional cookies
    if not popup_handled:
        decline_selectors = [
            ('button:has-text("Decline optional cookies")', "Decline optional cookies"),
        ]
        
        for selector, description in decline_selectors:
            try:
                if await page.locator(selector).is_visible(timeout=1000):
                    logger.info(f"Found: {description}")
                    await asyncio.sleep(_random_delay(0.5, 1.0))
                    await page.click(selector)
                    logger.info(f"Clicked '{description}'")
                    await asyncio.sleep(_random_delay(2.0, 3.0))
                    popup_handled = True
                    break
            except:
                pass
    
    # Save login info popup (mostly for logged in)
    try:
        if await page.locator('text="Save your login info?"').is_visible(timeout=1000):
            logger.info("Found 'Save Your Login Info' popup")
            not_now_buttons = await page.locator('button:has-text("Not Now")').all()
            if not_now_buttons:
                await asyncio.sleep(_random_delay(0.5, 1.0))
                await not_now_buttons[0].click()
                logger.info("Dismissed 'Save Login Info' popup")
                await asyncio.sleep(_random_delay(1.0, 2.0))
                popup_handled = True
    except:
        pass
    
    # Notifications popup
    try:
        if await page.locator('text="Turn on Notifications"').is_visible(timeout=1000):
            logger.info("Found 'Turn on Notifications' popup")
            not_now_buttons = await page.locator('button:has-text("Not Now")').all()
            if not_now_buttons:
                await asyncio.sleep(_random_delay(0.5, 1.0))
                await not_now_buttons[-1].click()
                logger.info("Dismissed notifications popup")
                await asyncio.sleep(_random_delay(1.0, 2.0))
                popup_handled = True
    except:
        pass
    
    # Anonymous login/signup popup (e.g., "Log in to continue" or "Sign up to see more")
    try:
        login_popup_indicators = [
            'text="Log in to continue"',
            'text="Sign up to see photos and videos from your friends"',
            'div[role="dialog"] >> text="Log in"',
        ]
        for indicator in login_popup_indicators:
            if await page.locator(indicator).is_visible(timeout=1000):
                logger.info("Found anonymous login/signup popup")
                # Try multiple dismiss selectors
                dismiss_selectors = [
                    'button:has-text("Not Now")',
                    'button:has-text("Maybe Later")',
                    'svg[aria-label="Close"]',
                    'button[aria-label="Close"]',
                    'div[role="dialog"] button:has-text("Dismiss")',
                ]
                for dismiss_selector in dismiss_selectors:
                    dismiss_buttons = await page.locator(dismiss_selector).all()
                    if dismiss_buttons:
                        await asyncio.sleep(_random_delay(0.5, 1.0))
                        await dismiss_buttons[0].click()
                        logger.info(f"Dismissed login popup using {dismiss_selector}")
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                        popup_handled = True
                        break
                if popup_handled:
                    break
                else:
                    logger.warning("Could not find dismiss button for login popup")
    except:
        pass
    
    if not popup_handled:
        logger.info("No popups found")
    
    return popup_handled

async def _handle_challenge(page):
    """Handle Instagram challenge"""
    logger = logging.getLogger(__name__)
    logger.warning("Handling security challenge...")
    
    await asyncio.sleep(_random_delay(1.0, 2.0))
    
    # Check for "This was me" button
    try:
        if await page.locator('button:has-text("This was me")').is_visible(timeout=3000):
            logger.info("Found 'This was me' button")
            await page.click('button:has-text("This was me")')
            logger.info("Clicked 'This was me'")
            await asyncio.sleep(_random_delay(3.0, 5.0))
            return True
    except:
        pass
    
    # Check for verification code input
    try:
        code_selector = 'input[name="verificationCode"], input[placeholder*="code" i]'
        if await page.locator(code_selector).is_visible(timeout=3000):
            logger.info("Verification code required")
            # Since headless, we can't input, but for completeness, assume manual if possible
            # In production, this might raise or log
            raise Exception("Verification code required - manual intervention needed")
            # code = input("\n📧 Enter verification code: ").strip()
            # await _human_like_type(page, code_selector, code)
            # await page.click('button[type="submit"]')
            # logger.info("Code submitted")
            # await asyncio.sleep(_random_delay(3.0, 5.0))
            # return True
    except Exception as e:
        logger.error(f"Challenge handling error: {e}")
    
    # Unknown challenge type
    logger.warning("Unknown challenge type - manual intervention needed")
    # input("⏸️  Complete the challenge manually, then press Enter...")
    raise Exception("Unknown challenge - manual intervention needed")
    
    return False

async def _handle_2fa(page):
    """Handle 2FA"""
    logger = logging.getLogger(__name__)
    logger.warning("Handling 2FA...")
    
    # Since headless, can't input, raise or log
    raise Exception("2FA code required - manual intervention needed")
    # code = input("\n🔐 Enter 2FA code: ").strip()
    # await _human_like_type(page, 'input[name="verificationCode"]', code)
    # await asyncio.sleep(_random_delay(0.5, 1.0))
    # await page.click('button[type="submit"]')
    # logger.info("2FA code submitted")
    # await asyncio.sleep(_random_delay(3.0, 5.0))
    return True

async def fetch_ig_urls(account: str, cookies: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch Instagram post URLs, captions, and media URLs using Playwright with stealth.
    Args:
    account: Instagram username (with or without @)
    cookies: Optional list of cookies for logged-in session (for private profiles)
    Returns:
    List of dicts with url, caption, media_url, is_video
    """
    account = account.lstrip('@')
    posts = []
    
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching posts for @{account}")
    
    # Random viewport
    viewport = _get_random_viewport()
    logger.info(f"Viewport: {viewport['width']}x{viewport['height']}")
    
    # User agent
    user_agent = _get_realistic_user_agent()
    logger.info(f"User agent: {user_agent[:50]}...")
    
    async with async_playwright() as p:
        # Launch browser with stealth args
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                f'--window-size={viewport["width"]},{viewport["height"]}'
            ]
        )
        
        # Create context with realistic settings
        context = await browser.new_context(
            viewport=viewport,
            locale='en-US',
            timezone_id='America/New_York',
            user_agent=user_agent,
            device_scale_factor=1,
            has_touch=False,
            java_script_enabled=True,
            permissions=['geolocation'],
            color_scheme='light',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        # Apply comprehensive stealth script
        await context.add_init_script(_get_enhanced_stealth_script())
        
        logger.info("Browser context configured with stealth")
        
        if cookies:
            await context.add_cookies(cookies)
            logger.info("Using logged-in session")
        
        page = await context.new_page()
        
        try:
            # Load profile page
            await page.goto(f'https://www.instagram.com/{account}/', 
                            wait_until='networkidle', 
                            timeout=60000)
            await asyncio.sleep(_random_delay(2.0, 4.0))
            
            current_url = page.url
            if '/challenge/' in current_url or 'suspicious' in current_url.lower():
                logger.warning("Challenge detected on profile load")
                handled = await _handle_challenge(page)
                if not handled:
                    raise Exception("Failed to handle challenge")
                # Reload after handle
                await page.goto(f'https://www.instagram.com/{account}/', 
                                wait_until='networkidle', 
                                timeout=60000)
            
            if await page.locator('input[name="verificationCode"], input[aria-label*="code"]').is_visible(timeout=3000):
                logger.warning("2FA detected on profile load")
                handled = await _handle_2fa(page)
                if not handled:
                    raise Exception("Failed to handle 2FA")
                # Reload after handle
                await page.goto(f'https://www.instagram.com/{account}/', 
                                wait_until='networkidle', 
                                timeout=60000)
            
            # Handle popups
            popup_attempts = 0
            max_popup_attempts = 5
            while popup_attempts < max_popup_attempts:
                popup_found = await _handle_popups(page)
                if popup_found:
                    popup_attempts += 1
                    logger.info(f"Popup handled, checking for more... (attempt {popup_attempts}/{max_popup_attempts})")
                    await asyncio.sleep(_random_delay(0.5, 1.5))
                else:
                    logger.info("All popups handled")
                    break
            
            # Simulate human behavior
            await _random_scroll(page)
            await asyncio.sleep(_random_delay(1.0, 2.0))
            
            # Check if private
            private_selector = 'h2:has-text("This account is private")'
            if await page.locator(private_selector).is_visible(timeout=5000):
                if not cookies:
                    logger.warning("Private profile - need logged-in session")
                    return []
                else:
                    # If cookies provided but still private, perhaps invalid
                    logger.warning("Private profile detected even with cookies - session may be invalid")
                    return []
            
            # Scroll to load posts (for limited number)
            loaded_posts = 0
            max_scroll_attempts = 20  # Prevent infinite loop
            scroll_attempts = 0
            
            while loaded_posts < config.POST_LIMIT and scroll_attempts < max_scroll_attempts:
                # Handle popups after each scroll (useful for anonymous login popups)
                await _handle_popups(page)
                
                # Find post links
                post_links_selector = 'a[href^="/p/"]'
                post_links = await page.locator(post_links_selector).all()
                
                # Process new links
                for link in post_links[loaded_posts:]:
                    if loaded_posts >= config.POST_LIMIT:
                        break
                    try:
                        post_url = await link.get_attribute('href')
                        post_url = f"https://www.instagram.com{post_url}"
                        
                        # Visit post
                        await page.goto(post_url, wait_until='networkidle', timeout=30000)
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                        
                        current_url = page.url
                        if '/challenge/' in current_url or 'suspicious' in current_url.lower():
                            logger.warning("Challenge detected on post load")
                            handled = await _handle_challenge(page)
                            if not handled:
                                raise Exception("Failed to handle challenge")
                            # Reload post
                            await page.goto(post_url, wait_until='networkidle', timeout=30000)
                        
                        if await page.locator('input[name="verificationCode"], input[aria-label*="code"]').is_visible(timeout=3000):
                            logger.warning("2FA detected on post load")
                            handled = await _handle_2fa(page)
                            if not handled:
                                raise Exception("Failed to handle 2FA")
                            # Reload post
                            await page.goto(post_url, wait_until='networkidle', timeout=30000)
                        
                        # Handle popups on post page
                        await _handle_popups(page)
                        
                        # Simulate human
                        await _random_scroll(page)
                        
                        # Extract caption
                        caption_selector = 'div[data-testid="post-comment-text"]'  # May need update if IG changes
                        caption = ""
                        try:
                            caption_elements = await page.locator(caption_selector).all()
                            if caption_elements:
                                caption = await caption_elements[0].inner_text()
                        except:
                            pass
                        
                        # Extract media URL and type
                        image_selector = 'img[alt*="image"]'  # Adjust for accuracy
                        video_selector = 'video[source]'
                        
                        is_video = False
                        media_url = ""
                        
                        try:
                            video_elements = await page.locator(video_selector).all()
                            if video_elements:
                                is_video = True
                                media_url = await video_elements[0].get_attribute('src')
                            else:
                                image_elements = await page.locator(image_selector).all()
                                if image_elements:
                                    media_url = await image_elements[0].get_attribute('src')
                        except:
                            pass
                        
                        if media_url:
                            posts.append({
                                "url": post_url,
                                "caption": caption.strip() if caption else "",
                                "media_url": media_url,
                                "is_video": is_video
                            })
                            loaded_posts += 1
                            logger.info(f"Processed post {loaded_posts}: {post_url}")
                        
                        # Go back to profile with human delay
                        await page.go_back(timeout=30000)
                        await asyncio.sleep(_random_delay(2.0, 3.0))
                        
                        # Handle any popups after back
                        await _handle_popups(page)
                        
                    except Exception as e:
                        logger.warning(f"Error processing post: {e}")
                        # Continue to next
                        await page.goto(f'https://www.instagram.com/{account}/', timeout=30000)
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                
                if loaded_posts >= config.POST_LIMIT:
                    break
                
                # Scroll down with human variation
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(_random_delay(2.0, 4.0))
                scroll_attempts += 1
                
                # Random extra scroll
                if random.random() > 0.5:
                    await _random_scroll(page)
            
            logger.info(f"Fetched {len(posts)} posts")
        
        except Exception as e:
            logger.error(f"Error fetching @{account}: {e}")
            # Optional: screenshot for debug
            # await page.screenshot(path=f'error_fetch_{account}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
        
        finally:
            await browser.close()
    
    return posts