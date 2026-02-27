from app.strategies.base import BaseSearchStrategy


INPUT_SELECTORS = list(dict.fromkeys([

    # Strong signals
    "input[type='search']",
    "input[name='q']",
    "input[name='query']",
    "input[name='s']",
    "input[name='search']",
    "input[name='keyword']",
    "input[name='keys']",

    # Extended search variations
    "input[name='searchTerm']",
    "input[name='search_query']",
    "input[name='searchKey']",
    "input[name='searchText']",
    "input[name='search_string']",
    "input[name='searchValue']",
    "input[name='search_for']",
    "input[name='search_term']",
    "input[name='search_keywords']",

    # Attribute-based
    "input[placeholder*='Search']",
    "input[placeholder*='search']",
    "input[class*='search']",
    "input[class*='Search']",
    "input[class*='query']",
    "input[id*='search']",
    "input[id*='Search']",
    "input[id*='query']",

    # Role-based
    "[role='search'] input",

    # Form-based
    "form[action*='search'] input",

    # Common IDs
    "#search",
    "#search-field",
    "#search-input",

    # Mobile
    ".mobile-search input",
    
    # Additional enterprise / CMS patterns

# WordPress common
"input[name='s']",
"input[id='s']",
"input[class*='wp-search']",
"input[class*='wp-block-search']",

# Drupal
"input[name='keys']",
"input[id*='edit-keys']",
"input[class*='block-search']",

# Joomla
"input[name='searchword']",
"input[id*='mod-search-searchword']",

# Magento / Ecommerce
"input[name='searchCriteria']",
"input[name='search_query']",
"input[class*='catalogsearch']",
"input[id*='search']",

# Shopify
"input[name='q']",
"input[id='Search-In-Modal']",
"input[id='Search-In-Drawer']",

# ASP.NET
"input[name='ctl00$Search']",
"input[id*='ctl00_Search']",

# SharePoint
"input[name='k']",
"input[id*='SearchBox']",

# Government portals
"input[name='txtSearch']",
"input[id*='txtSearch']",

# Enterprise portals
"input[name='searchText']",
"input[name='searchBox']",
"input[id*='searchBox']",
"input[class*='searchBox']",

# React / SPA patterns
"input[data-testid*='search']",
"input[data-test*='search']",
"input[data-qa*='search']",
"input[data-search]",
"input[aria-label*='Search']",
"input[aria-label*='search']",

# Placeholder variations
"input[placeholder*='Type to search']",
"input[placeholder*='Search here']",
"input[placeholder*='What are you looking']",
"input[placeholder*='Enter keyword']",
"input[placeholder*='Enter search']",

# Class-based broader detection
"input[class*='SearchField']",
"input[class*='SearchInput']",
"input[class*='SearchBox']",
"input[class*='SearchBar']",
"input[class*='site-search']",
"input[class*='header-search']",
"input[class*='nav-search']",

# ID-based broader detection
"input[id*='SearchField']",
"input[id*='SearchInput']",
"input[id*='SearchBox']",
"input[id*='SearchBar']",
"input[id*='site-search']",

# Data attribute enterprise patterns
"input[data-component*='search']",
"input[data-role*='search']",
"input[data-module*='search']",

# Algolia (very common)
"input[class*='ais-SearchBox-input']",
"input[id*='algolia']",

# Coveo (enterprise search)
"input[class*='coveo-search']",
"input[data-coveo]"

# ElasticSearch frontend patterns
"input[class*='elastic']",

# Sitecore
"input[name='SearchTerm']",

# Oracle / WebCenter
"input[name='searchString']",

# Government / EU portals
"input[name='queryText']",
"input[name='searchString']",
"input[name='search_phrase']",

# Hidden but activated later
"form[role='search'] input",
"form[class*='search'] input",

# Navbar-specific
"nav input[name='q']",
"header input[name='q']",

# Large news portals
"input[name='search_input']",
"input[name='search-field']",
"input[id='search-field']",

# Vue / Angular dynamic bindings
"input[v-model*='search']",
"input[ng-model*='search']",
]))


class InputSearchStrategy(BaseSearchStrategy):

    TEST_KEYWORD = "automationtest123"
    CONFIDENCE_THRESHOLD = 3

    async def attach_network_listener(self):
        self.captured_request = None

        def handler(request):
            url = request.url.lower()
            if any(k in url for k in ["search", "?q=", "?s=", "query="]):
                self.captured_request = request.url

        self.page.on("request", handler)

    async def execute(self):

        from app.executor import execute_search

        await self.attach_network_listener()

        for selector in INPUT_SELECTORS:

            try:
                element = await self.page.query_selector(selector)
                if not element:
                    continue

                if not await element.is_visible():
                    continue

                input_type = await element.get_attribute("type")
                if input_type in ["email", "password", "tel", "number"]:
                    continue

                await element.fill("")
                await element.fill(self.TEST_KEYWORD)

                # Try Enter first
                await element.press("Enter")

                try:
                    await self.page.wait_for_load_state(
                        "domcontentloaded",
                        timeout=5000
                    )
                except:
                    pass

                # Strongest signal: network call
                if self.captured_request:
                    return {
                        "method": "input",
                        "pattern": selector,
                        "confidence": 4,
                        "result_type": "network"
                    }

                # Fallback validation
                score, result_type = await execute_search(
                    self.page,
                    selector
                )

                if score >= self.CONFIDENCE_THRESHOLD:
                    return {
                        "method": "input",
                        "pattern": selector,
                        "confidence": score,
                        "result_type": result_type
                    }

                # Try clicking submit button if Enter didn't work
                try:
                    submit_btn = await self.page.query_selector(
                        "button[type='submit'], input[type='submit']"
                    )

                    if submit_btn and await submit_btn.is_visible():
                        await submit_btn.click()

                        try:
                            await self.page.wait_for_load_state(
                                "domcontentloaded",
                                timeout=5000
                            )
                        except:
                            pass

                        score, result_type = await execute_search(
                            self.page,
                            selector
                        )

                        if score >= self.CONFIDENCE_THRESHOLD:
                            return {
                                "method": "input",
                                "pattern": selector,
                                "confidence": score,
                                "result_type": result_type
                            }

                except:
                    pass

            except:
                continue

        return None