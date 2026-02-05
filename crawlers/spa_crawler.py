"""
SPA (Single Page Application) Crawler.

Specialized crawler for React, Vue, Angular, and other SPA frameworks.
Handles hydration, state management, and framework-specific patterns.
"""

import asyncio
import logging
import json
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable
from enum import Enum

from .playwright_crawler import PlaywrightCrawler, PlaywrightConfig, CrawlResult

logger = logging.getLogger(__name__)


class SPAFramework(str, Enum):
    """Supported SPA frameworks."""
    REACT = "react"
    VUE = "vue"
    ANGULAR = "angular"
    SVELTE = "svelte"
    NEXT = "next"  # Next.js
    NUXT = "nuxt"  # Nuxt.js
    UNKNOWN = "unknown"


@dataclass
class SPAConfig(PlaywrightConfig):
    """Configuration for SPA crawler."""

    # Framework detection
    framework: Optional[SPAFramework] = None
    auto_detect_framework: bool = True

    # Hydration settings
    wait_for_hydration: bool = True
    hydration_timeout: int = 10000  # ms

    # State extraction
    extract_state: bool = False
    state_keys: List[str] = field(default_factory=list)

    # Router handling
    wait_for_router: bool = True
    router_timeout: int = 5000  # ms

    # API interception
    intercept_api_calls: bool = False
    api_patterns: List[str] = field(default_factory=list)

    # SSR/SSG detection
    detect_ssr: bool = True

    def __post_init__(self):
        super().__post_init__()
        if not self.api_patterns:
            self.api_patterns = [
                "*/api/*",
                "*/_next/data/*",
                "*/graphql*",
                "*/__api/*"
            ]


@dataclass
class SPAState:
    """SPA application state."""
    framework: SPAFramework
    is_hydrated: bool
    is_ssr: bool
    state_data: Dict[str, Any]
    api_responses: List[Dict[str, Any]]
    route_info: Dict[str, Any]


class SPACrawler(PlaywrightCrawler):
    """
    Crawler specialized for Single Page Applications.

    Features:
    - Automatic framework detection
    - Hydration wait support
    - State extraction (Redux, Vuex, NgRx)
    - Router state handling
    - API call interception
    - SSR/SSG detection
    """

    def __init__(self, config: Optional[SPAConfig] = None):
        """
        Initialize SPA crawler.

        Args:
            config: SPA-specific configuration
        """
        self.spa_config = config or SPAConfig()
        super().__init__(self.spa_config)

        self._detected_framework: Optional[SPAFramework] = None
        self._intercepted_apis: List[Dict[str, Any]] = []
        self._is_hydrated = False
        self._is_ssr = False

    @property
    def framework(self) -> SPAFramework:
        """Get detected or configured framework."""
        return self._detected_framework or self.spa_config.framework or SPAFramework.UNKNOWN

    async def start(self) -> None:
        """Start browser with SPA-specific setup."""
        await super().start()

        # Setup API interception if enabled
        if self.spa_config.intercept_api_calls:
            await self._setup_api_interception()

    async def _setup_api_interception(self) -> None:
        """Setup API call interception."""
        self._ensure_page()

        async def handle_response(response):
            url = response.url
            for pattern in self.spa_config.api_patterns:
                # Simple pattern matching
                if self._match_pattern(url, pattern):
                    try:
                        body = await response.json()
                        self._intercepted_apis.append({
                            "url": url,
                            "status": response.status,
                            "data": body
                        })
                    except Exception:
                        pass
                    break

        self._page.on("response", handle_response)

    def _match_pattern(self, url: str, pattern: str) -> bool:
        """Simple wildcard pattern matching."""
        import re
        regex_pattern = pattern.replace("*", ".*")
        return bool(re.search(regex_pattern, url))

    async def navigate(
        self,
        url: str,
        wait_until: Optional[str] = None,
        timeout: Optional[int] = None
    ):
        """
        Navigate to URL with SPA-aware waiting.

        Args:
            url: Target URL
            wait_until: Wait condition override
            timeout: Timeout override
        """
        self._intercepted_apis.clear()

        response = await super().navigate(url, wait_until, timeout)

        # Detect framework if auto-detection is enabled
        if self.spa_config.auto_detect_framework:
            self._detected_framework = await self.detect_framework()
            logger.info(f"Detected SPA framework: {self._detected_framework}")

        # Detect SSR
        if self.spa_config.detect_ssr:
            self._is_ssr = await self._detect_ssr()
            logger.info(f"SSR detected: {self._is_ssr}")

        # Wait for hydration
        if self.spa_config.wait_for_hydration:
            await self.wait_for_hydration()

        # Wait for router
        if self.spa_config.wait_for_router:
            await self._wait_for_router()

        return response

    async def detect_framework(self) -> SPAFramework:
        """
        Detect the SPA framework used on the page.

        Returns:
            Detected SPAFramework enum value
        """
        self._ensure_page()

        detection_result = await self.evaluate("""
            () => {
                // React detection
                if (window.__REACT_DEVTOOLS_GLOBAL_HOOK__ ||
                    document.querySelector('[data-reactroot]') ||
                    document.querySelector('[data-react-root]') ||
                    document.querySelector('#__next')) {
                    // Check for Next.js specifically
                    if (window.__NEXT_DATA__ || document.querySelector('#__next')) {
                        return 'next';
                    }
                    return 'react';
                }

                // Vue detection
                if (window.__VUE__ ||
                    window.__VUE_DEVTOOLS_GLOBAL_HOOK__ ||
                    document.querySelector('[data-v-]') ||
                    document.querySelector('[data-server-rendered]')) {
                    // Check for Nuxt.js specifically
                    if (window.__NUXT__ || window.$nuxt) {
                        return 'nuxt';
                    }
                    return 'vue';
                }

                // Angular detection
                if (window.ng ||
                    window.getAllAngularRootElements ||
                    document.querySelector('[ng-version]') ||
                    document.querySelector('[_nghost]') ||
                    document.querySelector('app-root')) {
                    return 'angular';
                }

                // Svelte detection
                if (document.querySelector('[class*="svelte-"]') ||
                    window.__svelte) {
                    return 'svelte';
                }

                return 'unknown';
            }
        """)

        return SPAFramework(detection_result)

    async def wait_for_hydration(self, timeout: Optional[int] = None) -> bool:
        """
        Wait for SPA hydration to complete.

        Args:
            timeout: Timeout in ms

        Returns:
            True if hydration completed successfully
        """
        self._ensure_page()
        timeout = timeout or self.spa_config.hydration_timeout

        framework = self.framework

        try:
            if framework == SPAFramework.REACT or framework == SPAFramework.NEXT:
                await self._wait_for_react_hydration(timeout)
            elif framework == SPAFramework.VUE or framework == SPAFramework.NUXT:
                await self._wait_for_vue_hydration(timeout)
            elif framework == SPAFramework.ANGULAR:
                await self._wait_for_angular_hydration(timeout)
            elif framework == SPAFramework.SVELTE:
                await self._wait_for_svelte_hydration(timeout)
            else:
                # Generic hydration wait
                await self._wait_for_generic_hydration(timeout)

            self._is_hydrated = True
            logger.info("SPA hydration completed")
            return True

        except Exception as e:
            logger.warning(f"Hydration wait failed: {e}")
            self._is_hydrated = False
            return False

    async def _wait_for_react_hydration(self, timeout: int) -> None:
        """Wait for React hydration."""
        await self.wait_for_function(
            """
            () => {
                // Check if React has hydrated
                const root = document.getElementById('root') ||
                            document.getElementById('__next') ||
                            document.querySelector('[data-reactroot]');
                if (!root) return false;

                // Check for React Fiber
                const key = Object.keys(root).find(k => k.startsWith('__reactFiber') || k.startsWith('__reactContainer'));
                if (key) return true;

                // Check for event listeners as hydration indicator
                const buttons = document.querySelectorAll('button, a, input');
                for (const btn of buttons) {
                    const props = Object.keys(btn).filter(k => k.startsWith('__reactProps') || k.startsWith('__reactEventHandlers'));
                    if (props.length > 0) return true;
                }

                // Fallback: check for Next.js build manifest
                if (window.__NEXT_DATA__ && window.__NEXT_DATA__.buildId) {
                    return document.readyState === 'complete';
                }

                return false;
            }
            """,
            timeout=timeout
        )

    async def _wait_for_vue_hydration(self, timeout: int) -> None:
        """Wait for Vue hydration."""
        await self.wait_for_function(
            """
            () => {
                // Check for Vue 3
                if (window.__VUE__) {
                    const apps = document.querySelectorAll('[data-v-app]');
                    if (apps.length > 0) return true;
                }

                // Check for Nuxt
                if (window.__NUXT__) {
                    return window.__NUXT__.serverRendered !== undefined;
                }

                // Check for Vue instance
                if (window.$nuxt || window.Vue) {
                    return true;
                }

                // Check for Vue components
                const vueElements = document.querySelectorAll('[data-v-]');
                if (vueElements.length > 0) {
                    // Check if Vue has mounted
                    return document.querySelectorAll('.__vue__mounted').length > 0 ||
                           document.readyState === 'complete';
                }

                return false;
            }
            """,
            timeout=timeout
        )

    async def _wait_for_angular_hydration(self, timeout: int) -> None:
        """Wait for Angular hydration."""
        await self.wait_for_function(
            """
            () => {
                // Check for Angular platform stability
                if (window.getAllAngularRootElements) {
                    const roots = window.getAllAngularRootElements();
                    return roots && roots.length > 0;
                }

                // Check for Angular components
                const appRoot = document.querySelector('app-root, [ng-version]');
                if (appRoot) {
                    // Check if Angular has bootstrapped
                    const ngVersion = appRoot.getAttribute('ng-version');
                    return ngVersion !== null;
                }

                return false;
            }
            """,
            timeout=timeout
        )

    async def _wait_for_svelte_hydration(self, timeout: int) -> None:
        """Wait for Svelte hydration."""
        await self.wait_for_function(
            """
            () => {
                // Svelte components have special class attributes
                const svelteElements = document.querySelectorAll('[class*="svelte-"]');
                if (svelteElements.length > 0) {
                    return document.readyState === 'complete';
                }
                return false;
            }
            """,
            timeout=timeout
        )

    async def _wait_for_generic_hydration(self, timeout: int) -> None:
        """Generic hydration wait for unknown frameworks."""
        # Wait for document ready and network idle
        await self.wait_for_load_state("networkidle", timeout)

        # Additional wait for dynamic content
        await self.wait(500)

    async def _wait_for_router(self) -> None:
        """Wait for SPA router to be ready."""
        self._ensure_page()

        framework = self.framework

        try:
            if framework == SPAFramework.REACT or framework == SPAFramework.NEXT:
                await self._wait_for_react_router()
            elif framework == SPAFramework.VUE or framework == SPAFramework.NUXT:
                await self._wait_for_vue_router()
            elif framework == SPAFramework.ANGULAR:
                await self._wait_for_angular_router()
        except Exception as e:
            logger.debug(f"Router wait skipped: {e}")

    async def _wait_for_react_router(self) -> None:
        """Wait for React Router."""
        await self.wait_for_function(
            """
            () => {
                // Check for React Router
                if (window.__REACT_ROUTER__ || window.ReactRouter) {
                    return true;
                }

                // Check for Next.js router
                if (window.__NEXT_DATA__) {
                    return true;
                }

                // Check for location change handlers
                return document.readyState === 'complete';
            }
            """,
            timeout=self.spa_config.router_timeout
        )

    async def _wait_for_vue_router(self) -> None:
        """Wait for Vue Router."""
        await self.wait_for_function(
            """
            () => {
                // Check for Vue Router
                if (window.$nuxt && window.$nuxt.$route) {
                    return true;
                }
                if (window.VueRouter) {
                    return true;
                }
                return document.readyState === 'complete';
            }
            """,
            timeout=self.spa_config.router_timeout
        )

    async def _wait_for_angular_router(self) -> None:
        """Wait for Angular Router."""
        await self.wait_for_function(
            """
            () => {
                if (window.ng && window.ng.probe) {
                    return true;
                }
                return document.readyState === 'complete';
            }
            """,
            timeout=self.spa_config.router_timeout
        )

    async def _detect_ssr(self) -> bool:
        """Detect if page is server-side rendered."""
        return await self.evaluate("""
            () => {
                // Check Next.js SSR
                if (window.__NEXT_DATA__) {
                    return window.__NEXT_DATA__.props !== undefined;
                }

                // Check Nuxt SSR
                if (window.__NUXT__) {
                    return window.__NUXT__.serverRendered === true;
                }

                // Check for data-server-rendered attribute (Vue)
                if (document.querySelector('[data-server-rendered]')) {
                    return true;
                }

                // Check if there's meaningful content before JS execution
                const rootContent = document.body.innerHTML;
                // If page has substantial content, likely SSR
                return rootContent.length > 1000;
            }
        """)

    async def extract_state(self) -> Dict[str, Any]:
        """
        Extract application state from SPA.

        Returns:
            Dictionary containing state data
        """
        self._ensure_page()

        framework = self.framework
        state = {}

        if framework == SPAFramework.REACT or framework == SPAFramework.NEXT:
            state = await self._extract_react_state()
        elif framework == SPAFramework.VUE or framework == SPAFramework.NUXT:
            state = await self._extract_vue_state()
        elif framework == SPAFramework.ANGULAR:
            state = await self._extract_angular_state()

        # Filter by configured keys if specified
        if self.spa_config.state_keys:
            state = {k: v for k, v in state.items() if k in self.spa_config.state_keys}

        return state

    async def _extract_react_state(self) -> Dict[str, Any]:
        """Extract React application state."""
        return await self.evaluate("""
            () => {
                const state = {};

                // Next.js data
                if (window.__NEXT_DATA__) {
                    state.nextData = window.__NEXT_DATA__;
                }

                // Redux store
                try {
                    const root = document.getElementById('root') || document.getElementById('__next');
                    if (root) {
                        const key = Object.keys(root).find(k => k.startsWith('__reactContainer'));
                        if (key) {
                            const fiber = root[key];
                            // Try to find Redux store in context
                            let current = fiber;
                            while (current) {
                                if (current.memoizedState && current.memoizedState.store) {
                                    state.redux = current.memoizedState.store.getState();
                                    break;
                                }
                                current = current.return;
                            }
                        }
                    }
                } catch (e) {}

                // Check for window-level stores
                if (window.__REDUX_DEVTOOLS_EXTENSION__) {
                    try {
                        const stores = window.__REDUX_DEVTOOLS_EXTENSION__.open();
                        if (stores) state.reduxDevtools = 'available';
                    } catch (e) {}
                }

                // Zustand stores (commonly on window)
                for (const key of Object.keys(window)) {
                    if (key.includes('store') || key.includes('Store')) {
                        try {
                            const store = window[key];
                            if (store && typeof store.getState === 'function') {
                                state[key] = store.getState();
                            }
                        } catch (e) {}
                    }
                }

                return state;
            }
        """)

    async def _extract_vue_state(self) -> Dict[str, Any]:
        """Extract Vue application state."""
        return await self.evaluate("""
            () => {
                const state = {};

                // Nuxt state
                if (window.__NUXT__) {
                    state.nuxt = window.__NUXT__;
                }

                // Vuex store
                if (window.$nuxt && window.$nuxt.$store) {
                    state.vuex = window.$nuxt.$store.state;
                }

                // Pinia stores (Vue 3)
                if (window.__pinia) {
                    state.pinia = {};
                    for (const [id, store] of window.__pinia._s) {
                        state.pinia[id] = store.$state;
                    }
                }

                // Vue instance data
                try {
                    const vueApps = document.querySelectorAll('[data-v-app]');
                    vueApps.forEach((app, index) => {
                        if (app.__vue_app__) {
                            state[`vueApp${index}`] = 'present';
                        }
                    });
                } catch (e) {}

                return state;
            }
        """)

    async def _extract_angular_state(self) -> Dict[str, Any]:
        """Extract Angular application state."""
        return await self.evaluate("""
            () => {
                const state = {};

                // NgRx store
                try {
                    if (window.ng && window.ng.probe) {
                        const roots = window.getAllAngularRootElements();
                        if (roots && roots.length > 0) {
                            const component = window.ng.probe(roots[0]);
                            if (component && component.injector) {
                                // Try to get store
                                try {
                                    const store = component.injector.get('Store');
                                    if (store) {
                                        state.ngrx = 'present';
                                    }
                                } catch (e) {}
                            }
                        }
                    }
                } catch (e) {}

                return state;
            }
        """)

    async def get_route_info(self) -> Dict[str, Any]:
        """
        Get current route information.

        Returns:
            Route information dictionary
        """
        self._ensure_page()

        return await self.evaluate("""
            () => {
                const info = {
                    url: window.location.href,
                    pathname: window.location.pathname,
                    search: window.location.search,
                    hash: window.location.hash
                };

                // Next.js router
                if (window.__NEXT_DATA__) {
                    info.nextRoute = window.__NEXT_DATA__.page;
                    info.nextQuery = window.__NEXT_DATA__.query;
                }

                // Nuxt router
                if (window.$nuxt && window.$nuxt.$route) {
                    info.nuxtRoute = {
                        name: window.$nuxt.$route.name,
                        path: window.$nuxt.$route.path,
                        params: window.$nuxt.$route.params,
                        query: window.$nuxt.$route.query
                    };
                }

                return info;
            }
        """)

    async def navigate_spa_route(
        self,
        path: str,
        wait_for_content: Optional[str] = None
    ) -> None:
        """
        Navigate to SPA route without full page reload.

        Args:
            path: Route path
            wait_for_content: Selector to wait for after navigation
        """
        self._ensure_page()
        self._intercepted_apis.clear()

        # Use pushState for client-side navigation
        await self.evaluate(f"""
            () => {{
                // Try framework-specific navigation first
                if (window.__NEXT_DATA__ && window.next && window.next.router) {{
                    window.next.router.push('{path}');
                    return;
                }}

                if (window.$nuxt && window.$nuxt.$router) {{
                    window.$nuxt.$router.push('{path}');
                    return;
                }}

                // Fallback to History API
                window.history.pushState(null, '', '{path}');
                window.dispatchEvent(new PopStateEvent('popstate'));
            }}
        """)

        # Wait for navigation
        await self.wait(500)
        await self.wait_for_network_idle(timeout=5000)

        # Wait for hydration after route change
        if self.spa_config.wait_for_hydration:
            await self.wait_for_hydration()

        # Wait for specific content
        if wait_for_content:
            await self.wait_for_selector(wait_for_content)

    async def get_spa_state(self) -> SPAState:
        """
        Get comprehensive SPA state information.

        Returns:
            SPAState object with all state data
        """
        state_data = {}
        if self.spa_config.extract_state:
            state_data = await self.extract_state()

        route_info = await self.get_route_info()

        return SPAState(
            framework=self.framework,
            is_hydrated=self._is_hydrated,
            is_ssr=self._is_ssr,
            state_data=state_data,
            api_responses=self._intercepted_apis.copy(),
            route_info=route_info
        )

    async def crawl(
        self,
        url: str,
        fields: List[Dict[str, str]],
        wait_selector: Optional[str] = None,
        pre_actions: Optional[List[Dict[str, Any]]] = None,
        post_actions: Optional[List[Dict[str, Any]]] = None
    ) -> CrawlResult:
        """
        Execute a complete SPA crawl operation.

        Args:
            url: Target URL
            fields: List of field definitions
            wait_selector: Selector to wait for
            pre_actions: Actions before extraction
            post_actions: Actions after extraction

        Returns:
            CrawlResult with extracted data and SPA metadata
        """
        result = await super().crawl(
            url,
            fields,
            wait_selector,
            pre_actions,
            post_actions
        )

        # Add SPA-specific metadata
        if result.success:
            spa_state = await self.get_spa_state()
            result.metadata.update({
                "spa_framework": spa_state.framework.value,
                "is_hydrated": spa_state.is_hydrated,
                "is_ssr": spa_state.is_ssr,
                "route_info": spa_state.route_info,
            })

            if self.spa_config.extract_state:
                result.metadata["app_state"] = spa_state.state_data

            if self.spa_config.intercept_api_calls:
                result.metadata["api_calls"] = spa_state.api_responses

        return result

    async def extract_lazy_loaded_content(
        self,
        trigger_selector: str,
        content_selector: str,
        timeout: int = 5000
    ) -> Optional[str]:
        """
        Extract content that is lazy-loaded after interaction.

        Args:
            trigger_selector: Element to interact with to trigger loading
            content_selector: Selector for the content to extract
            timeout: Wait timeout in ms

        Returns:
            Extracted content or None
        """
        self._ensure_page()

        try:
            # Scroll trigger into view
            await self.scroll_to_element(trigger_selector)
            await self.wait(300)

            # Click or hover to trigger loading
            try:
                await self.click(trigger_selector, timeout=1000)
            except Exception:
                await self.hover(trigger_selector)

            # Wait for content
            await self.wait_for_selector(content_selector, timeout=timeout)

            return await self.get_text(content_selector)

        except Exception as e:
            logger.warning(f"Failed to extract lazy-loaded content: {e}")
            return None

    async def handle_virtual_scroll(
        self,
        container_selector: str,
        item_selector: str,
        max_items: int = 100,
        scroll_amount: int = 500
    ) -> List[Dict[str, Any]]:
        """
        Handle virtual/windowed scrolling in SPAs.

        Virtual scrolling only renders visible items, requiring special handling.

        Args:
            container_selector: Scroll container selector
            item_selector: Items selector within container
            max_items: Maximum items to extract
            scroll_amount: Pixels to scroll per iteration

        Returns:
            List of extracted items
        """
        self._ensure_page()
        seen_items = {}
        items = []

        # Get initial items
        current_items = await self._page.query_selector_all(item_selector)

        while len(items) < max_items:
            # Extract current visible items
            for element in current_items:
                # Get unique identifier for deduplication
                text = await element.inner_text()
                item_hash = hash(text.strip())

                if item_hash not in seen_items:
                    seen_items[item_hash] = True
                    html = await element.inner_html()
                    items.append({
                        "text": text.strip(),
                        "html": html
                    })

                    if len(items) >= max_items:
                        break

            if len(items) >= max_items:
                break

            # Scroll container
            await self.evaluate(f"""
                () => {{
                    const container = document.querySelector('{container_selector}');
                    if (container) {{
                        container.scrollTop += {scroll_amount};
                    }}
                }}
            """)

            await self.wait(300)

            # Get new items
            new_items = await self._page.query_selector_all(item_selector)

            # Check if we got new items
            if len(new_items) == len(current_items):
                # Check if we've reached the end
                new_texts = set()
                for el in new_items:
                    t = await el.inner_text()
                    new_texts.add(t.strip())

                old_texts = set()
                for el in current_items:
                    t = await el.inner_text()
                    old_texts.add(t.strip())

                if new_texts == old_texts:
                    # No new content, we've reached the end
                    break

            current_items = new_items

        return items[:max_items]
