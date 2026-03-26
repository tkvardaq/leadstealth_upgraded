import asyncio
import random
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from utils import install_playwright


class LeadScraper:
    def __init__(self, headful=False):
        self.headful = headful
        self.playwright = None
        self.browser = None
        self.context = None
        self.enrich_context = None

    async def start_browser(self):
        install_playwright()
        if not self.playwright:
            self.playwright = await async_playwright().start()

        if not self.browser:
            self.browser = await self.playwright.chromium.launch(
                headless=not self.headful,
                args=["--disable-blink-features=AutomationControlled"],
            )
            self.context = await self.browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            self.enrich_context = await self.browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )

    async def close_browser(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except:
            pass

    async def _delay(self, lo=1.0, hi=3.0):
        await asyncio.sleep(random.uniform(lo, hi))

    def _find_emails(self, text):
        raw = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text))
        bad = (
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".svg",
            ".css",
            ".js",
            ".woff",
            ".ttf",
            ".ico",
        )
        skip = (
            "example",
            "sentry",
            "noreply",
            "no-reply",
            "mailer-daemon",
            "postmaster",
        )
        return [
            e
            for e in raw
            if not e.endswith(bad) and not any(s in e.lower() for s in skip)
        ]

    def _find_obfuscated_emails(self, html):
        emails = set()
        patterns = [
            r"([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\s+at\s+)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\s+dot\s+)\s*([a-zA-Z]{2,})",
            r"([a-zA-Z0-9._%+-]+)\s*@\s*([a-zA-Z0-9.-]+)\s*\.\s*([a-zA-Z]{2,})",
        ]
        for pattern in patterns:
            for m in re.finditer(pattern, html, re.IGNORECASE):
                emails.add(f"{m.group(1)}@{m.group(2)}.{m.group(3)}")
        return list(emails)

    def _find_socials(self, html):
        soc = {
            "facebook": None,
            "instagram": None,
            "linkedin": None,
            "twitter": None,
            "youtube": None,
            "tiktok": None,
            "pinterest": None,
        }
        if not html:
            return soc
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            h = a["href"].lower().split("?")[0].rstrip("/")
            if (
                "facebook.com/" in h
                and "/sharer" not in h
                and "/share" not in h
                and not soc["facebook"]
            ):
                soc["facebook"] = a["href"]
            elif "instagram.com/" in h and not soc["instagram"] and h.count("/") >= 3:
                soc["instagram"] = a["href"]
            elif "linkedin.com/" in h and "/share" not in h and not soc["linkedin"]:
                soc["linkedin"] = a["href"]
            elif (
                ("twitter.com/" in h or "x.com/" in h)
                and not soc["twitter"]
                and h.count("/") >= 3
            ):
                soc["twitter"] = a["href"]
            elif (
                "youtube.com/" in h
                and not soc["youtube"]
                and ("channel" in h or "user" in h or "@" in h)
            ):
                soc["youtube"] = a["href"]
            elif "tiktok.com/" in h and not soc["tiktok"] and "@" in h:
                soc["tiktok"] = a["href"]
            elif "pinterest.com/" in h and not soc["pinterest"]:
                soc["pinterest"] = a["href"]
        return soc

    def _detect_tech(self, html):
        """Simple tech detection based on HTML markers"""
        tech = {
            "cms": None,
            "wordpress": False,
            "shopify": False,
            "wix": False,
            "squarespace": False,
            "technologies": []
        }
        if not html:
            return tech
        
        lower_html = html.lower()
        if "wp-content" in lower_html or "wp-includes" in lower_html:
            tech["cms"] = "WordPress"
            tech["wordpress"] = True
            tech["technologies"].append("WordPress")
        elif "myshopify.com" in lower_html or 'cdn.shopify.com' in lower_html:
            tech["cms"] = "Shopify"
            tech["shopify"] = True
            tech["technologies"].append("Shopify")
        elif "wix.com" in lower_html or "_wix" in lower_html:
            tech["cms"] = "Wix"
            tech["wix"] = True
            tech["technologies"].append("Wix")
        elif "static1.squarespace.com" in lower_html:
            tech["cms"] = "Squarespace"
            tech["squarespace"] = True
            tech["technologies"].append("Squarespace")
            
        return tech

    def _parse_address(self, address_str):
        city, state, zip = "", "", ""
        if not address_str:
            return city, state, zip
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", address_str)
        if zip_match:
            zip = zip_match.group(1)
        state_match = re.search(r",\s*([A-Z]{2})\s+\d{5}", address_str)
        if not state_match:
            state_match = re.search(r",\s*([A-Z]{2})\b", address_str)
        if state_match:
            state = state_match.group(1)
        if state:
            city_match = re.search(r",\s*([^,]+),\s*" + re.escape(state), address_str)
            if city_match:
                city = city_match.group(1).strip()
        return city, state, zip

    async def enrich_website(self, base_url):
        enriched = {
            "email": None,
            "facebook": None,
            "instagram": None,
            "linkedin": None,
            "twitter": None,
            "youtube": None,
            "tiktok": None,
            "pinterest": None,
            "cms": None,
            "wordpress": False,
            "shopify": False,
            "technologies": "",
        }
        if not base_url or not base_url.startswith("http"):
            return enriched

        await self.start_browser()
        page = await self.enrich_context.new_page()
        await Stealth().apply_stealth_async(page)

        parsed = urlparse(base_url)
        domain_base = f"{parsed.scheme}://{parsed.netloc}"
        urls = [
            base_url,
            urljoin(domain_base, "/contact"),
            urljoin(domain_base, "/about"),
        ]

        emails = set()
        socials = {k: None for k in enriched if k != "email"}

        try:
            for url in urls:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    await asyncio.sleep(1)
                    html = await page.content()
                    emails.update(self._find_emails(html))
                    emails.update(self._find_obfuscated_emails(html))
                    
                    # Detect tech on first page (usually home)
                    if url == base_url:
                        tech = self._detect_tech(html)
                        enriched.update({
                            "cms": tech["cms"],
                            "wordpress": tech["wordpress"],
                            "shopify": tech["shopify"],
                            "technologies": ", ".join(tech["technologies"])
                        })
                    
                    ps = self._find_socials(html)
                    for k, v in ps.items():
                        if v and not socials.get(k):
                            socials[k] = v
                    if emails and all(socials.values()):
                        break
                except:
                    continue
        finally:
            await page.close()

        if emails:
            generic = ["info@", "admin@", "support@", "sales@", "hello@", "contact@"]
            specific = [
                e for e in emails if not any(e.lower().startswith(g) for g in generic)
            ]
            enriched["email"] = specific[0] if specific else sorted(emails)[0]
        enriched.update(socials)
        return enriched

    async def search_google_maps(self, query, location):
        await self.start_browser()
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)

        search_term = f"{query} in {location}".replace(" ", "+")
        url = f"https://www.google.com/maps/search/{search_term}/"
        print(f"  [Maps] Navigating to: {url}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await self._delay(3, 5)

            for text in ["Accept all", "I agree", "Agree"]:
                btn = page.locator(f'button:has-text("{text}")')
                if await btn.count() > 0:
                    await btn.first.click()
                    await self._delay(2, 3)
                    break

            feed = page.locator('div[role="feed"]')
            if await feed.count() > 0:
                for i in range(15):
                    await feed.evaluate("node => node.scrollBy(0, 3000)")
                    await asyncio.sleep(1.5)
                    if await page.locator("span.HlvSq").count() > 0:
                        break

            all_links = await page.locator('a[href*="/maps/place/"]').all()
            link_data = []
            seen = set()
            for link in all_links:
                name = await link.get_attribute("aria-label")
                href = await link.get_attribute("href")
                if name and name not in seen:
                    seen.add(name)
                    link_data.append({"name": name, "href": href, "element": link})

            for i, item in enumerate(link_data):
                try:
                    await item["element"].click()
                    await self._delay(2, 3)

                    phone, website, address, category, rating, reviews = (
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                    )

                    phone_el = page.locator(
                        'button[data-tooltip="Copy phone number"], [data-item-id="phone"]'
                    )
                    if await phone_el.count() > 0:
                        phone = await phone_el.first.inner_text()

                    site_el = page.locator('a[data-tooltip="Open website"]')
                    if await site_el.count() > 0:
                        website = await site_el.first.get_attribute("href")

                    addr_el = page.locator('button[data-tooltip="Copy address"]')
                    if await addr_el.count() > 0:
                        address = await addr_el.first.inner_text()
                    
                    # Hours extraction
                    hours = ""
                    hours_el = page.locator('div[aria-label*="hours"]')
                    if await hours_el.count() > 0:
                        hours = await hours_el.first.get_attribute("aria-label")

                    cat_el = page.locator('button[jsaction="pane.rating.category"]')
                    if await cat_el.count() > 0:
                        category = await cat_el.first.inner_text()

                    rating_box = page.locator(".F7nice")
                    if await rating_box.count() > 0:
                        txt = await rating_box.first.inner_text()
                        parts = txt.split("\n")
                        if len(parts) >= 1:
                            rating = parts[0].strip()
                        if len(parts) >= 2:
                            reviews = parts[1].strip("()")

                    city, state, zip = self._parse_address(address)

                    yield {
                        "name": item["name"],
                        "source": "Google Maps",
                        "website": website,
                        "phone": phone,
                        "address": address,
                        "city": city,
                        "state": state,
                        "zip": zip,
                        "category": category,
                        "rating": rating,
                        "review_count": reviews,
                        "hours": hours,
                        "google_maps_url": item["href"]
                    }
                except:
                    continue
        finally:
            await page.close()

    async def search_yellowpages(self, query, location):
        await self.start_browser()
        page = await self.context.new_page()
        await stealth(page)
        search_term = query.replace(" ", "+")
        loc_term = location.replace(" ", "+")
        for pg in range(1, 4):
            url = f"https://www.yellowpages.com/search?search_terms={search_term}&geo_location_terms={loc_term}&page={pg}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                listings = soup.select("div.result")
                for listing in listings:
                    try:
                        name = listing.select_one("a.business-name").get_text(
                            strip=True
                        )
                        phone = (
                            listing.select_one("div.phones").get_text(strip=True)
                            if listing.select_one("div.phones")
                            else ""
                        )
                        site_el = listing.select_one("a.track-visit-website")
                        website = site_el["href"] if site_el else ""
                        addr = (
                            listing.select_one("div.adr").get_text(strip=True)
                            if listing.select_one("div.adr")
                            else ""
                        )
                        cats = ", ".join(
                            [
                                c.get_text(strip=True)
                                for c in listing.select(".categories a")
                            ]
                        )
                        city, state, zip = self._parse_address(addr)
                        yield {
                            "name": name,
                            "source": "Yellow Pages",
                            "website": website,
                            "phone": phone,
                            "address": addr,
                            "city": city,
                            "state": state,
                            "zip": zip,
                            "category": cats,
                        }
                    except:
                        continue
                if not soup.select_one("a.next"):
                    break
            except:
                break
        await page.close()

    async def search_yelp(self, query, location):
        await self.start_browser()
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        search_term = query.replace(" ", "+")
        loc_term = location.replace(" ", "+")
        for pg in range(0, 3):
            url = f"https://www.yelp.com/search?find_desc={search_term}&find_loc={loc_term}&start={pg * 10}"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                listings = soup.select('[data-testid="serp-ia-card"]')
                for listing in listings:
                    try:
                        name_el = listing.select_one('a[href*="/biz/"]')
                        name = name_el.get_text(strip=True)
                        biz_url = "https://www.yelp.com" + name_el["href"]
                        phone = (
                            listing.select_one('p:has(a[href^="tel:"])').get_text(
                                strip=True
                            )
                            if listing.select_one('p:has(a[href^="tel:"])')
                            else ""
                        )
                        addr = (
                            listing.select_one("address").get_text(strip=True)
                            if listing.select_one("address")
                            else ""
                        )
                        cats = ", ".join(
                            [
                                c.get_text(strip=True)
                                for c in listing.select('span[class*="css-"] a')
                            ]
                        )
                        city, state, zip = self._parse_address(addr)
                        
                        # Note: Website on Yelp is often hidden. 
                        # To find it accurately we would need to visit the biz_url.
                        # For now we'll yield the basic data and the enhancer will handle the rest.
                        
                        yield {
                            "name": name,
                            "source": "Yelp",
                            "website": "",
                            "phone": phone,
                            "address": addr,
                            "city": city,
                            "state": state,
                            "zip": zip,
                            "category": cats,
                            "yelp_url": biz_url,
                        }
                    except:
                        continue
            except:
                break
        await page.close()
