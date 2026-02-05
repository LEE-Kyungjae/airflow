"""
Authenticated Crawler using Playwright.

This module provides browser-based crawling capabilities for sites
that require authentication, including form login, OAuth, and session management.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any, Callable, Awaitable
from pathlib import Path
from enum import Enum

try:
    from playwright.async_api import (
        async_playwright,
        Browser,
        BrowserContext,
        Page,
        Playwright,
        TimeoutError as PlaywrightTimeout,
        Error as PlaywrightError
    )
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from .session_manager import SessionManager, SessionState, AuthCredentials, AuthType

logger = logging.getLogger(__name__)


class BrowserType(str, Enum):
    """Supported browser types."""
    CHROMIUM = "chromium"
    FIREFOX = "firefox"
    WEBKIT = "webkit"


@dataclass
class PlaywrightConfig:
    """Configuration for Playwright browser."""
    browser_type: BrowserType = BrowserType.CHROMIUM
    headless: bool = True
    slow_mo: int = 0  # Milliseconds to slow down operations
    timeout: int = 30000  # Default timeout in milliseconds
    viewport_width: int = 1920
    viewport_height: int = 1080
    user_agent: Optional[str] = None
    proxy: Optional[Dict[str, str]] = None
    ignore_https_errors: bool = False
    extra_http_headers: Optional[Dict[str, str]] = None
    locale: str = "ko-KR"
    timezone_id: str = "Asia/Seoul"
    geolocation: Optional[Dict[str, float]] = None
    permissions: List[str] = field(default_factory=list)
    downloads_path: Optional[str] = None
    record_video_dir: Optional[str] = None
    record_video_size: Optional[Dict[str, int]] = None
    trace_dir: Optional[str] = None

    def to_browser_args(self) -> Dict[str, Any]:
        """Convert to Playwright browser launch arguments."""
        args = {
            "headless": self.headless,
            "slow_mo": self.slow_mo
        }

        if self.proxy:
            args["proxy"] = self.proxy

        return args

    def to_context_args(self) -> Dict[str, Any]:
        """Convert to Playwright browser context arguments."""
        args = {
            "viewport": {
                "width": self.viewport_width,
                "height": self.viewport_height
            },
            "ignore_https_errors": self.ignore_https_errors,
            "locale": self.locale,
            "timezone_id": self.timezone_id
        }

        if self.user_agent:
            args["user_agent"] = self.user_agent

        if self.extra_http_headers:
            args["extra_http_headers"] = self.extra_http_headers

        if self.geolocation:
            args["geolocation"] = self.geolocation

        if self.permissions:
            args["permissions"] = self.permissions

        if self.record_video_dir:
            args["record_video_dir"] = self.record_video_dir
            if self.record_video_size:
                args["record_video_size"] = self.record_video_size

        return args


@dataclass
class LoginResult:
    """Result of a login attempt."""
    success: bool
    message: str = ""
    error_code: Optional[str] = None
    requires_captcha: bool = False
    requires_2fa: bool = False
    session_saved: bool = False
    cookies_count: int = 0
    execution_time_ms: int = 0


class AuthenticatedCrawler:
    """
    Browser-based crawler for authenticated sites using Playwright.

    Features:
    - Form-based login with configurable selectors
    - OAuth provider support (Google, GitHub, Kakao, Naver)
    - API key and bearer token authentication
    - Session state persistence and restoration
    - CAPTCHA and 2FA handling hooks
    - Automatic session refresh
    - Screenshot capture for debugging

    Usage:
        session_manager = SessionManager(encryption_key="...")
        config = PlaywrightConfig(headless=True)

        async with AuthenticatedCrawler(session_manager, config) as crawler:
            # Try to restore existing session
            if not await crawler.restore_session("my_source"):
                # Login required
                await crawler.login_form(
                    url="https://example.com/login",
                    username_selector="#email",
                    password_selector="#password",
                    submit_selector="button[type=submit]",
                    username="user@example.com",
                    password="secret",
                    success_indicator=".user-dashboard"
                )
                await crawler.save_current_session("my_source")

            # Now crawl authenticated pages
            page = await crawler.get_page()
            await page.goto("https://example.com/protected")
            content = await page.content()
    """

    # OAuth provider configurations
    OAUTH_PROVIDERS = {
        "google": {
            "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
            "token_url": "https://oauth2.googleapis.com/token",
            "email_selector": "input[type='email']",
            "password_selector": "input[type='password']",
            "next_button": "#identifierNext",
            "password_next": "#passwordNext"
        },
        "github": {
            "authorize_url": "https://github.com/login/oauth/authorize",
            "token_url": "https://github.com/login/oauth/access_token",
            "email_selector": "#login_field",
            "password_selector": "#password",
            "submit_selector": "input[type='submit']"
        },
        "kakao": {
            "authorize_url": "https://kauth.kakao.com/oauth/authorize",
            "email_selector": "#loginId--1",
            "password_selector": "#password--2",
            "submit_selector": "button.btn_g.btn_confirm"
        },
        "naver": {
            "authorize_url": "https://nid.naver.com/oauth2.0/authorize",
            "email_selector": "#id",
            "password_selector": "#pw",
            "submit_selector": "button.btn_login"
        }
    }

    def __init__(
        self,
        session_manager: SessionManager,
        config: Optional[PlaywrightConfig] = None
    ):
        """
        Initialize AuthenticatedCrawler.

        Args:
            session_manager: SessionManager for credential and session storage.
            config: Playwright configuration. Uses defaults if not provided.
        """
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is required for AuthenticatedCrawler. "
                "Install with: pip install playwright && playwright install"
            )

        self.session_manager = session_manager
        self.config = config or PlaywrightConfig()

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._start_time: Optional[float] = None

        # Hooks for custom handling
        self._captcha_handler: Optional[Callable[[Page], Awaitable[bool]]] = None
        self._2fa_handler: Optional[Callable[[Page, str], Awaitable[bool]]] = None

    async def __aenter__(self) -> 'AuthenticatedCrawler':
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def start(self) -> None:
        """Start Playwright and launch browser."""
        self._playwright = await async_playwright().start()

        # Select browser type
        if self.config.browser_type == BrowserType.FIREFOX:
            browser_cls = self._playwright.firefox
        elif self.config.browser_type == BrowserType.WEBKIT:
            browser_cls = self._playwright.webkit
        else:
            browser_cls = self._playwright.chromium

        # Launch browser
        self._browser = await browser_cls.launch(**self.config.to_browser_args())
        logger.info(f"Browser launched: {self.config.browser_type.value}")

    async def close(self) -> None:
        """Close browser and cleanup resources."""
        if self._page:
            await self._page.close()
            self._page = None

        if self._context:
            await self._context.close()
            self._context = None

        if self._browser:
            await self._browser.close()
            self._browser = None

        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

        logger.info("Browser closed")

    async def _ensure_context(self) -> BrowserContext:
        """Ensure browser context exists."""
        if not self._browser:
            raise RuntimeError("Browser not started. Call start() first.")

        if not self._context:
            self._context = await self._browser.new_context(
                **self.config.to_context_args()
            )
            self._context.set_default_timeout(self.config.timeout)

        return self._context

    async def get_page(self) -> Page:
        """
        Get or create the current page.

        Returns:
            Playwright Page object.
        """
        context = await self._ensure_context()

        if not self._page:
            self._page = await context.new_page()

        return self._page

    def _start_timer(self) -> None:
        """Start execution timer."""
        self._start_time = time.time()

    def _get_elapsed_ms(self) -> int:
        """Get elapsed time in milliseconds."""
        if self._start_time is None:
            return 0
        return int((time.time() - self._start_time) * 1000)

    # =========================================================================
    # Form-based Login
    # =========================================================================

    async def login_form(
        self,
        url: str,
        username_selector: str,
        password_selector: str,
        submit_selector: str,
        username: str,
        password: str,
        success_indicator: Optional[str] = None,
        failure_indicator: Optional[str] = None,
        pre_login_actions: Optional[List[Dict[str, Any]]] = None,
        wait_after_submit: int = 3000,
        screenshot_on_failure: bool = True
    ) -> LoginResult:
        """
        Perform form-based login.

        Args:
            url: Login page URL.
            username_selector: CSS selector for username/email input.
            password_selector: CSS selector for password input.
            submit_selector: CSS selector for submit button.
            username: Username or email to enter.
            password: Password to enter.
            success_indicator: CSS selector that appears on successful login.
            failure_indicator: CSS selector that appears on failed login.
            pre_login_actions: List of actions to perform before login
                (e.g., clicking "login" button to show form).
            wait_after_submit: Milliseconds to wait after submit.
            screenshot_on_failure: Whether to capture screenshot on failure.

        Returns:
            LoginResult with success status and details.
        """
        self._start_timer()
        page = await self.get_page()

        try:
            # Navigate to login page
            logger.info(f"Navigating to login page: {url}")
            await page.goto(url, wait_until="networkidle")

            # Execute pre-login actions if any
            if pre_login_actions:
                for action in pre_login_actions:
                    await self._execute_action(page, action)

            # Wait for login form to be visible
            await page.wait_for_selector(username_selector, state="visible")

            # Fill credentials
            logger.info("Filling login credentials")
            await page.fill(username_selector, username)
            await page.fill(password_selector, password)

            # Check for CAPTCHA before submitting
            if await self._detect_captcha(page):
                logger.warning("CAPTCHA detected")
                if self._captcha_handler:
                    captcha_resolved = await self._captcha_handler(page)
                    if not captcha_resolved:
                        return LoginResult(
                            success=False,
                            message="CAPTCHA resolution failed",
                            error_code="CAPTCHA_FAILED",
                            requires_captcha=True,
                            execution_time_ms=self._get_elapsed_ms()
                        )
                else:
                    return LoginResult(
                        success=False,
                        message="CAPTCHA detected but no handler configured",
                        error_code="CAPTCHA_REQUIRED",
                        requires_captcha=True,
                        execution_time_ms=self._get_elapsed_ms()
                    )

            # Submit form
            logger.info("Submitting login form")
            await page.click(submit_selector)

            # Wait for navigation/response
            await asyncio.sleep(wait_after_submit / 1000)

            # Check for 2FA
            if await self._detect_2fa(page):
                logger.warning("2FA required")
                return LoginResult(
                    success=False,
                    message="2FA verification required",
                    error_code="2FA_REQUIRED",
                    requires_2fa=True,
                    execution_time_ms=self._get_elapsed_ms()
                )

            # Check for failure indicator
            if failure_indicator:
                try:
                    failure_elem = await page.query_selector(failure_indicator)
                    if failure_elem:
                        error_text = await failure_elem.inner_text()
                        if screenshot_on_failure:
                            await self._capture_screenshot(page, "login_failed")
                        return LoginResult(
                            success=False,
                            message=f"Login failed: {error_text}",
                            error_code="LOGIN_FAILED",
                            execution_time_ms=self._get_elapsed_ms()
                        )
                except Exception:
                    pass

            # Check for success indicator
            if success_indicator:
                try:
                    await page.wait_for_selector(
                        success_indicator,
                        state="visible",
                        timeout=10000
                    )
                    logger.info("Login successful - success indicator found")
                except PlaywrightTimeout:
                    if screenshot_on_failure:
                        await self._capture_screenshot(page, "login_no_indicator")
                    return LoginResult(
                        success=False,
                        message="Login indicator not found after submit",
                        error_code="NO_SUCCESS_INDICATOR",
                        execution_time_ms=self._get_elapsed_ms()
                    )

            # Get cookies count
            cookies = await self._context.cookies()

            return LoginResult(
                success=True,
                message="Login successful",
                cookies_count=len(cookies),
                execution_time_ms=self._get_elapsed_ms()
            )

        except PlaywrightTimeout as e:
            logger.error(f"Login timeout: {e}")
            if screenshot_on_failure:
                await self._capture_screenshot(page, "login_timeout")
            return LoginResult(
                success=False,
                message=f"Login timeout: {str(e)}",
                error_code="TIMEOUT",
                execution_time_ms=self._get_elapsed_ms()
            )
        except PlaywrightError as e:
            logger.error(f"Login error: {e}")
            if screenshot_on_failure:
                await self._capture_screenshot(page, "login_error")
            return LoginResult(
                success=False,
                message=f"Login error: {str(e)}",
                error_code="BROWSER_ERROR",
                execution_time_ms=self._get_elapsed_ms()
            )

    async def _execute_action(self, page: Page, action: Dict[str, Any]) -> None:
        """Execute a page action."""
        action_type = action.get("type", "click")
        selector = action.get("selector")
        value = action.get("value")
        wait = action.get("wait", 500)

        if action_type == "click" and selector:
            await page.click(selector)
        elif action_type == "fill" and selector and value:
            await page.fill(selector, value)
        elif action_type == "wait":
            await asyncio.sleep(wait / 1000)
        elif action_type == "wait_for_selector" and selector:
            await page.wait_for_selector(selector, state="visible")

        await asyncio.sleep(wait / 1000)

    # =========================================================================
    # OAuth Login
    # =========================================================================

    async def login_oauth(
        self,
        provider: str,
        credentials: Dict[str, str],
        callback_url: Optional[str] = None,
        client_id: Optional[str] = None,
        scope: Optional[str] = None
    ) -> LoginResult:
        """
        Perform OAuth-based login.

        Args:
            provider: OAuth provider name (google, github, kakao, naver).
            credentials: Dictionary with username/email and password.
            callback_url: OAuth callback URL (for authorization flow).
            client_id: OAuth client ID (for authorization flow).
            scope: OAuth scope (for authorization flow).

        Returns:
            LoginResult with success status and details.
        """
        self._start_timer()

        if provider.lower() not in self.OAUTH_PROVIDERS:
            return LoginResult(
                success=False,
                message=f"Unsupported OAuth provider: {provider}",
                error_code="UNSUPPORTED_PROVIDER",
                execution_time_ms=self._get_elapsed_ms()
            )

        provider_config = self.OAUTH_PROVIDERS[provider.lower()]
        page = await self.get_page()

        try:
            # Navigate to authorization URL
            auth_url = provider_config["authorize_url"]
            if client_id and callback_url:
                auth_url += f"?client_id={client_id}&redirect_uri={callback_url}"
                if scope:
                    auth_url += f"&scope={scope}"
                auth_url += "&response_type=code"

            logger.info(f"Navigating to OAuth provider: {provider}")
            await page.goto(auth_url, wait_until="networkidle")

            # Fill credentials based on provider
            email_selector = provider_config.get("email_selector")
            password_selector = provider_config.get("password_selector")

            if email_selector and password_selector:
                # Fill email/username
                await page.wait_for_selector(email_selector, state="visible")
                await page.fill(email_selector, credentials.get("username", ""))

                # Handle Google's two-step form
                if provider.lower() == "google":
                    await page.click(provider_config["next_button"])
                    await page.wait_for_selector(password_selector, state="visible")

                # Fill password
                await page.fill(password_selector, credentials.get("password", ""))

                # Submit
                submit_selector = provider_config.get("submit_selector")
                if submit_selector:
                    await page.click(submit_selector)
                elif provider.lower() == "google":
                    await page.click(provider_config["password_next"])

            # Wait for redirect or callback
            await asyncio.sleep(3)

            # Check for 2FA
            if await self._detect_2fa(page):
                return LoginResult(
                    success=False,
                    message="2FA verification required",
                    error_code="2FA_REQUIRED",
                    requires_2fa=True,
                    execution_time_ms=self._get_elapsed_ms()
                )

            cookies = await self._context.cookies()

            return LoginResult(
                success=True,
                message=f"OAuth login successful via {provider}",
                cookies_count=len(cookies),
                execution_time_ms=self._get_elapsed_ms()
            )

        except PlaywrightTimeout as e:
            logger.error(f"OAuth login timeout: {e}")
            return LoginResult(
                success=False,
                message=f"OAuth login timeout: {str(e)}",
                error_code="TIMEOUT",
                execution_time_ms=self._get_elapsed_ms()
            )
        except PlaywrightError as e:
            logger.error(f"OAuth login error: {e}")
            return LoginResult(
                success=False,
                message=f"OAuth login error: {str(e)}",
                error_code="BROWSER_ERROR",
                execution_time_ms=self._get_elapsed_ms()
            )

    # =========================================================================
    # API Key / Bearer Token
    # =========================================================================

    async def login_api_key(
        self,
        api_key: str,
        header_name: str = "Authorization",
        header_prefix: str = "Bearer "
    ) -> None:
        """
        Configure API key authentication via headers.

        Args:
            api_key: API key or token.
            header_name: Header name (default: Authorization).
            header_prefix: Header value prefix (default: "Bearer ").
        """
        context = await self._ensure_context()

        # Set extra HTTP headers
        headers = {header_name: f"{header_prefix}{api_key}"}
        await context.set_extra_http_headers(headers)

        logger.info(f"API key authentication configured for header: {header_name}")

    async def login_basic_auth(self, username: str, password: str) -> None:
        """
        Configure HTTP Basic authentication.

        Args:
            username: Basic auth username.
            password: Basic auth password.
        """
        import base64
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()

        context = await self._ensure_context()
        await context.set_extra_http_headers({
            "Authorization": f"Basic {credentials}"
        })

        logger.info("Basic authentication configured")

    # =========================================================================
    # Session Management
    # =========================================================================

    async def restore_session(self, source_id: str) -> bool:
        """
        Restore a previously saved session.

        Args:
            source_id: Source identifier to restore session for.

        Returns:
            True if session restored successfully, False otherwise.
        """
        session = await self.session_manager.load_session(source_id)

        if not session:
            logger.info(f"No valid session found for source: {source_id}")
            return False

        try:
            context = await self._ensure_context()

            # Restore cookies
            if session.cookies:
                cookies_list = []
                for name, cookie_data in session.cookies.items():
                    if isinstance(cookie_data, dict):
                        cookies_list.append(cookie_data)
                    else:
                        # Simple name:value format
                        cookies_list.append({
                            "name": name,
                            "value": str(cookie_data),
                            "domain": "",
                            "path": "/"
                        })

                if cookies_list:
                    await context.add_cookies(cookies_list)

            # Restore local storage via page
            if session.local_storage:
                page = await self.get_page()
                for key, value in session.local_storage.items():
                    await page.evaluate(
                        f"window.localStorage.setItem('{key}', '{value}')"
                    )

            # Restore session storage
            if session.session_storage:
                page = await self.get_page()
                for key, value in session.session_storage.items():
                    await page.evaluate(
                        f"window.sessionStorage.setItem('{key}', '{value}')"
                    )

            # Set extra headers if any
            if session.headers:
                await context.set_extra_http_headers(session.headers)

            # Mark session as used
            session.mark_used()
            await self.session_manager.save_session(session)

            logger.info(f"Session restored for source: {source_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to restore session: {e}")
            return False

    async def save_current_session(
        self,
        source_id: str,
        duration_hours: int = 24
    ) -> None:
        """
        Save current browser session state.

        Args:
            source_id: Source identifier.
            duration_hours: Session validity duration in hours.
        """
        context = await self._ensure_context()
        page = await self.get_page()

        # Get cookies
        cookies = await context.cookies()
        cookies_dict = {c["name"]: c for c in cookies}

        # Get local storage
        local_storage = await page.evaluate("""
            () => {
                const items = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    items[key] = localStorage.getItem(key);
                }
                return items;
            }
        """)

        # Get session storage
        session_storage = await page.evaluate("""
            () => {
                const items = {};
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    items[key] = sessionStorage.getItem(key);
                }
                return items;
            }
        """)

        # Create session state
        session = SessionState(
            source_id=source_id,
            cookies=cookies_dict,
            local_storage=local_storage if local_storage else None,
            session_storage=session_storage if session_storage else None,
            user_agent=self.config.user_agent,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=duration_hours),
            is_valid=True,
            login_verified_at=datetime.utcnow()
        )

        await self.session_manager.save_session(session, duration_hours)
        logger.info(f"Session saved for source: {source_id}")

    async def check_login_status(
        self,
        indicator_selector: str,
        check_url: Optional[str] = None
    ) -> bool:
        """
        Check if currently logged in by verifying indicator presence.

        Args:
            indicator_selector: CSS selector that appears when logged in.
            check_url: URL to navigate to for checking (optional).

        Returns:
            True if logged in, False otherwise.
        """
        page = await self.get_page()

        try:
            if check_url:
                await page.goto(check_url, wait_until="networkidle")

            element = await page.query_selector(indicator_selector)
            return element is not None

        except Exception as e:
            logger.error(f"Error checking login status: {e}")
            return False

    # =========================================================================
    # CAPTCHA and 2FA Handling
    # =========================================================================

    def set_captcha_handler(
        self,
        handler: Callable[[Page], Awaitable[bool]]
    ) -> None:
        """
        Set custom CAPTCHA handler.

        Args:
            handler: Async function that receives Page and returns True if resolved.
        """
        self._captcha_handler = handler

    def set_2fa_handler(
        self,
        handler: Callable[[Page, str], Awaitable[bool]]
    ) -> None:
        """
        Set custom 2FA handler.

        Args:
            handler: Async function that receives Page and selector, returns True if resolved.
        """
        self._2fa_handler = handler

    async def handle_captcha(
        self,
        captcha_selector: str,
        timeout_seconds: int = 300
    ) -> bool:
        """
        Handle CAPTCHA with user intervention request.

        This method pauses execution and waits for manual CAPTCHA resolution.
        In headless mode, this will time out unless a handler is configured.

        Args:
            captcha_selector: CSS selector for CAPTCHA element.
            timeout_seconds: Maximum wait time for resolution.

        Returns:
            True if CAPTCHA resolved, False if timeout.
        """
        page = await self.get_page()

        logger.warning(
            f"CAPTCHA detected. Manual intervention required. "
            f"Timeout in {timeout_seconds} seconds."
        )

        if self._captcha_handler:
            return await self._captcha_handler(page)

        # Wait for CAPTCHA element to disappear (user resolved it)
        try:
            await page.wait_for_selector(
                captcha_selector,
                state="hidden",
                timeout=timeout_seconds * 1000
            )
            logger.info("CAPTCHA resolved")
            return True
        except PlaywrightTimeout:
            logger.error("CAPTCHA resolution timeout")
            return False

    async def handle_2fa(
        self,
        code_input_selector: str,
        submit_selector: Optional[str] = None,
        timeout_seconds: int = 300
    ) -> bool:
        """
        Handle 2FA with user intervention request.

        Args:
            code_input_selector: CSS selector for 2FA code input.
            submit_selector: CSS selector for submit button (optional).
            timeout_seconds: Maximum wait time for code entry.

        Returns:
            True if 2FA completed, False if timeout.
        """
        page = await self.get_page()

        logger.warning(
            f"2FA verification required. "
            f"Timeout in {timeout_seconds} seconds."
        )

        if self._2fa_handler:
            return await self._2fa_handler(page, code_input_selector)

        # Wait for input to be filled and form submitted
        try:
            await page.wait_for_selector(
                code_input_selector,
                state="hidden",
                timeout=timeout_seconds * 1000
            )
            logger.info("2FA completed")
            return True
        except PlaywrightTimeout:
            logger.error("2FA resolution timeout")
            return False

    async def _detect_captcha(self, page: Page) -> bool:
        """Detect common CAPTCHA patterns."""
        captcha_selectors = [
            "iframe[src*='recaptcha']",
            "iframe[src*='hcaptcha']",
            ".g-recaptcha",
            ".h-captcha",
            "[data-sitekey]",
            "#captcha",
            ".captcha"
        ]

        for selector in captcha_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    return True
            except Exception:
                continue

        return False

    async def _detect_2fa(self, page: Page) -> bool:
        """Detect common 2FA patterns."""
        tfa_patterns = [
            "input[name*='otp']",
            "input[name*='2fa']",
            "input[name*='code']",
            "input[name*='verification']",
            "input[placeholder*='인증']",
            "input[placeholder*='코드']",
            "[data-testid*='2fa']",
            ".two-factor",
            "#two-factor"
        ]

        for selector in tfa_patterns:
            try:
                element = await page.query_selector(selector)
                if element and await element.is_visible():
                    # Check if it looks like a 2FA input
                    input_type = await element.get_attribute("type")
                    if input_type in ["text", "tel", "number", None]:
                        return True
            except Exception:
                continue

        return False

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def _capture_screenshot(
        self,
        page: Page,
        name: str
    ) -> Optional[str]:
        """Capture screenshot for debugging."""
        if not self.config.trace_dir:
            return None

        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            path = Path(self.config.trace_dir) / f"{name}_{timestamp}.png"
            path.parent.mkdir(parents=True, exist_ok=True)

            await page.screenshot(path=str(path), full_page=True)
            logger.info(f"Screenshot saved: {path}")
            return str(path)

        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")
            return None

    async def get_cookies(self) -> List[Dict[str, Any]]:
        """Get current browser cookies."""
        context = await self._ensure_context()
        return await context.cookies()

    async def clear_cookies(self) -> None:
        """Clear all browser cookies."""
        context = await self._ensure_context()
        await context.clear_cookies()
        logger.info("Cookies cleared")

    async def set_cookies(self, cookies: List[Dict[str, Any]]) -> None:
        """Set browser cookies."""
        context = await self._ensure_context()
        await context.add_cookies(cookies)

    async def navigate(
        self,
        url: str,
        wait_until: str = "networkidle"
    ) -> None:
        """
        Navigate to URL.

        Args:
            url: Target URL.
            wait_until: Wait condition (load, domcontentloaded, networkidle).
        """
        page = await self.get_page()
        await page.goto(url, wait_until=wait_until)

    async def get_content(self) -> str:
        """Get current page HTML content."""
        page = await self.get_page()
        return await page.content()

    async def evaluate(self, script: str) -> Any:
        """
        Evaluate JavaScript in page context.

        Args:
            script: JavaScript code to evaluate.

        Returns:
            Script return value.
        """
        page = await self.get_page()
        return await page.evaluate(script)
