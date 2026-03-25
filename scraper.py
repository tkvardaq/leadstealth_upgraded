import time
import random
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import urllib.parse
from utils import install_playwright


class LeadScraper:
    def __init__(self, headful=False):
        self.headful = headful
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.enrich_page = None

    def start_browser(self):
        install_playwright()
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=not self.headful)
        self.context = self.browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/Chicago",
        )
        self.page = self.context.new_page()
        Stealth().apply_stealth_sync(self.page)
        self.enrich_page = self.context.new_page()
        Stealth().apply_stealth_sync(self.enrich_page)

    def close_browser(self):
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except:
            pass

    def _delay(self, lo=1.0, hi=3.0):
        time.sleep(random.uniform(lo, hi))

    def _scroll(self, n=2):
        if not self.page:
            return
        for _ in range(n):
            try:
                self.page.mouse.move(random.randint(200, 900), random.randint(200, 500))
                self.page.evaluate(f"window.scrollBy(0, {random.randint(300, 600)})")
                time.sleep(random.uniform(0.5, 1.2))
            except:
                break

    # ── Extraction helpers ────────────────────────────────────
    def _parse_address(self, address_str):
        """Parse a full address string into components: city, state, zip_code."""
        city = ""
        state = ""
        zip_code = ""
        if not address_str:
            return city, state, zip_code

        # Try to extract zip code
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", address_str)
        if zip_match:
            zip_code = zip_match.group(1)

        # Try to extract state (2-letter abbreviation)
        state_match = re.search(r",\s*([A-Z]{2})\s+\d{5}", address_str)
        if state_match:
            state = state_match.group(1)
        else:
            state_match = re.search(r",\s*([A-Z]{2})\b", address_str)
            if state_match:
                state = state_match.group(1)

        # Try to extract city (text before state)
        if state:
            city_match = re.search(r",\s*([^,]+),\s*" + re.escape(state), address_str)
            if city_match:
                city = city_match.group(1).strip()

        return city, state, zip_code

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
        """Find emails written as 'name [at] domain [dot] com' or similar."""
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
                if any(c in h for c in ["/pages/", "/pg/", "/profile"]):
                    soc["facebook"] = a["href"]
                elif h.count("/") >= 3:
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
        return soc

    # ── Fast Enrichment (checks multiple pages) ────────────────
    def enrich_website(self, base_url):
        enriched = {
            "email": None,
            "facebook": None,
            "instagram": None,
            "linkedin": None,
            "twitter": None,
            "youtube": None,
        }
        if not base_url or not base_url.startswith("http"):
            return enriched
        if not self.enrich_page:
            self.start_browser()

        parsed = urlparse(base_url)
        domain_base = f"{parsed.scheme}://{parsed.netloc}"

        urls = [
            base_url,
            urljoin(domain_base, "/contact"),
            urljoin(domain_base, "/contact-us"),
            urljoin(domain_base, "/contactus"),
            urljoin(domain_base, "/about"),
            urljoin(domain_base, "/about-us"),
        ]

        emails = set()
        socials = {k: None for k in enriched if k != "email"}

        for url in urls:
            try:
                self.enrich_page.goto(url, wait_until="domcontentloaded", timeout=8000)
                time.sleep(random.uniform(0.5, 1.5))
                html = self.enrich_page.content()

                emails.update(self._find_emails(html))
                emails.update(self._find_obfuscated_emails(html))

                ps = self._find_socials(html)
                for k, v in ps.items():
                    if v and not socials.get(k):
                        socials[k] = v

                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("mailto:"):
                        em = href.replace("mailto:", "").split("?")[0].strip()
                        if "@" in em and "." in em:
                            emails.add(em)
                    elif href.startswith("tel:") and not enriched.get("phone"):
                        pass

                if emails and all(socials.values()):
                    break
            except:
                continue

        if emails:
            generic = [
                "info@",
                "admin@",
                "noreply@",
                "no-reply@",
                "support@",
                "sales@",
                "webmaster@",
                "postmaster@",
            ]
            specific = [
                e for e in emails if not any(e.lower().startswith(g) for g in generic)
            ]
            enriched["email"] = specific[0] if specific else sorted(emails)[0]

        enriched.update(socials)
        return enriched

    # ── Google Maps ───────────────────────────────────────────
    def search_google_maps(self, query, location):
        """Scrapes Google Maps. Collects ALL listings first, then extracts details."""
        if not self.page:
            self.start_browser()

        search_term = f"{query} in {location}".replace(" ", "+")
        url = f"https://www.google.com/maps/search/{search_term}/"
        print(f"  [Maps] Navigating to: {url}")

        try:
            self._delay(2, 4)
            self.page.goto(url, wait_until="domcontentloaded", timeout=40000)
            self._delay(3, 5)

            try:
                for text in ["Accept all", "I agree", "Agree"]:
                    btn = self.page.locator(f'button:has-text("{text}")')
                    if btn.count() > 0:
                        btn.first.click()
                        self._delay(2, 3)
                        break
            except:
                pass

            content = self.page.content()
            if "Pardon our interruption" in content or "unusual traffic" in content:
                print("  [Maps] CAPTCHA detected.")
                if self.headful:
                    time.sleep(30)
                else:
                    return

            try:
                feed = self.page.locator('div[role="feed"]')
                if feed.count() > 0:
                    for i in range(20):
                        feed.evaluate("node => node.scrollBy(0, 3000)")
                        time.sleep(random.uniform(0.8, 1.5))
                        try:
                            if self.page.locator("span.HlvSq").count() > 0:
                                print(f"    Feed end after {i + 1} scrolls")
                                break
                        except:
                            pass
                        if (i + 1) % 5 == 0:
                            print(f"    Scrolled {i + 1}x...")
                else:
                    self._scroll(5)
            except:
                pass

            all_links = self.page.locator('a[href*="/maps/place/"]').all()
            print(f"    Found {len(all_links)} place links")

            seen = set()
            link_data = []
            for link in all_links:
                try:
                    name = link.get_attribute("aria-label")
                    href = link.get_attribute("href") or ""
                    if not name or len(name.strip()) < 2:
                        continue
                    name = name.strip()
                    if name.lower() in seen:
                        continue
                    seen.add(name.lower())
                    link_data.append({"name": name, "href": href, "element": link})
                except:
                    continue

            print(f"    {len(link_data)} unique businesses")

            for i, item in enumerate(link_data):
                name = item["name"]
                el = item["element"]
                phone = ""
                website = ""
                address = ""
                category = ""
                rating = ""
                reviews = ""
                hours = ""
                description = ""

                try:
                    el.click(force=True)
                    self._delay(1.5, 3.0)

                    try:
                        self.page.wait_for_selector(
                            'button[data-tooltip="Copy phone number"], a[data-tooltip="Open website"], [data-item-id="phone"]',
                            timeout=4000,
                        )
                    except:
                        pass

                    for sel in [
                        'button[data-tooltip="Copy phone number"]',
                        '[data-item-id="phone"] .Io6YTe',
                        'a[href^="tel:"]',
                    ]:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                raw = (
                                    el2.first.get_attribute("aria-label")
                                    or el2.first.inner_text()
                                    or ""
                                )
                                if "tel:" in sel:
                                    raw = el2.first.get_attribute("href") or ""
                                    raw = raw.replace("tel:", "")
                                m = re.search(r"[\d()+.\-\s]{7,}", raw)
                                if m:
                                    phone = m.group().strip()
                                    break
                        except:
                            continue

                    for sel in [
                        'a[data-tooltip="Open website"]',
                        '[data-item-id="authority"] a',
                        'a[aria-label*="website" i]',
                    ]:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                website = el2.first.get_attribute("href") or ""
                                if website:
                                    break
                        except:
                            continue

                    for sel in [
                        'button[data-tooltip="Copy address"]',
                        '[data-item-id="address"]',
                    ]:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                val = (
                                    el2.first.get_attribute("aria-label")
                                    or el2.first.inner_text()
                                )
                                if val:
                                    address = (
                                        val.replace("Address: ", "")
                                        .replace("Copy address ", "")
                                        .strip()
                                    )
                                    break
                        except:
                            pass

                    for sel in [
                        'button[jsaction="pane.rating.category"]',
                        ".fontBodyMedium.mgr77e",
                    ]:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                category = el2.first.inner_text().strip()
                                break
                        except:
                            pass

                    try:
                        el2 = self.page.locator(".F7nice")
                        if el2.count() > 0:
                            txt = el2.first.inner_text()
                            parts = txt.split("\n")
                            if len(parts) >= 1:
                                rating = parts[0].strip()
                            if len(parts) >= 2:
                                reviews = (
                                    parts[1].replace("(", "").replace(")", "").strip()
                                )
                    except:
                        pass

                    try:
                        el2 = self.page.locator(
                            '[class*="open"], [aria-label*="hours" i]'
                        )
                        if el2.count() > 0:
                            hours = el2.first.inner_text().strip()[:100]
                    except:
                        pass

                    # Description snippet
                    for sel in [
                        '[class*="editorial"], [data-attrid="description"]',
                        ".fontBodyMedium",
                    ]:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                desc = el2.first.inner_text().strip()
                                if len(desc) > 20:
                                    description = desc[:300]
                                    break
                        except:
                            pass

                    try:
                        back = self.page.locator('button[aria-label="Back"]')
                        if back.count() > 0:
                            back.first.click()
                            self._delay(0.8, 1.5)
                    except:
                        pass

                except Exception:
                    try:
                        self.page.go_back()
                        self._delay(1.0, 2.0)
                    except:
                        pass

                city, state, zip_code = self._parse_address(address)

                lead = {
                    "name": name,
                    "source": "Google Maps",
                    "website": website.strip() if website else "",
                    "phone": phone.strip() if phone else "",
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "rating": rating,
                    "reviews": reviews,
                    "category": category,
                    "hours": hours,
                    "description": description,
                }
                print(
                    f"    [{i + 1}/{len(link_data)}] {name} | {phone or 'N/A'} | {(website[:35] + '...') if website else 'N/A'}"
                )
                yield lead

        except Exception as e:
            print(f"  Maps error: {e}")

        print(f"  [Maps] Done.")

    # ── Yellow Pages (Multi-page) ─────────────────────────────
    def search_yellowpages(self, query, location):
        """Scrapes Yellow Pages across multiple pages."""
        if not self.page:
            self.start_browser()

        search_term = query.replace(" ", "+")
        loc_term = location.replace(", ", "+").replace(" ", "+")

        seen = set()

        for pg in range(1, 6):
            url = f"https://www.yellowpages.com/search?search_terms={search_term}&geo_location_terms={loc_term}&page={pg}"
            print(f"  [YP] Page {pg}: {url}")

            try:
                self._delay(2, 4)
                self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                self._delay(2, 4)

                content = self.page.content()
                if any(
                    m in content.lower()
                    for m in ["checking your browser", "access denied", "captcha"]
                ):
                    print(f"  [YP] Blocked on page {pg}")
                    if self.headful:
                        time.sleep(20)
                    else:
                        break

                self._scroll(3)
                html = self.page.content()
                soup = BeautifulSoup(html, "html.parser")

                listings = soup.select("div.result")
                if not listings:
                    listings = soup.select("div.v-card")
                if not listings:
                    listings = soup.select('[class*="srp-listing"]')

                if not listings:
                    print(f"    No listings on page {pg}, stopping.")
                    break

                print(f"    {len(listings)} listings on page {pg}")

                count = 0
                for listing in listings:
                    try:
                        name_el = (
                            listing.select_one("a.business-name span")
                            or listing.select_one("a.business-name")
                            or listing.select_one('[class*="business-name"]')
                            or listing.select_one("h2 a")
                        )
                        if not name_el:
                            continue
                        name = name_el.get_text(strip=True)
                        if not name or len(name) < 2:
                            continue
                        if name.lower() in seen:
                            continue
                        seen.add(name.lower())

                        phone = ""
                        ph = listing.select_one("div.phones") or listing.select_one(
                            '[class*="phone"]'
                        )
                        if ph:
                            phone = ph.get_text(strip=True)

                        website = ""
                        for a in listing.select("a[href]"):
                            href = a.get("href", "")
                            cls = " ".join(a.get("class", []))

                            if "track-visit-website" in cls:
                                if (
                                    href.startswith("http")
                                    and "yellowpages" not in href.lower()
                                ):
                                    website = href
                                    break
                                elif "/redirect?" in href:
                                    website = self._yp_redirect(href) or ""
                                    if website:
                                        break

                            if "/redirect?" in href and not website:
                                r = self._yp_redirect(href)
                                if r:
                                    website = r
                                    break

                        if not website:
                            for a in listing.select("a[href]"):
                                if a.get_text(strip=True).lower() in (
                                    "website",
                                    "visit website",
                                ):
                                    href = a.get("href", "")
                                    if "/redirect?" in href:
                                        website = self._yp_redirect(href) or ""
                                    elif (
                                        href.startswith("http")
                                        and "yellowpages" not in href.lower()
                                    ):
                                        website = href
                                    if website:
                                        break

                        address = ""
                        adr_el = listing.select_one(".street-address")
                        loc_el = listing.select_one(".locality")
                        if adr_el:
                            address += adr_el.get_text(strip=True)
                        if loc_el:
                            address += ", " + loc_el.get_text(strip=True)

                        category = ""
                        cat_els = listing.select(".categories a")
                        if cat_els:
                            category = ", ".join(
                                [c.get_text(strip=True) for c in cat_els]
                            )

                        city, state, zip_code = self._parse_address(address.strip(", "))

                        # Extract rating/reviews from YP
                        rating = ""
                        reviews = ""
                        rating_el = listing.select_one('[class*="rating"]')
                        if rating_el:
                            m = re.search(r"(\d+\.?\d*)", rating_el.get_text())
                            if m:
                                rating = m.group(1)
                        rev_el = listing.select_one('[class*="count"]')
                        if rev_el:
                            m = re.search(r"(\d+)", rev_el.get_text())
                            if m:
                                reviews = m.group(1)

                        lead = {
                            "name": name,
                            "source": "Yellow Pages",
                            "website": website,
                            "phone": phone,
                            "address": address.strip(", "),
                            "city": city,
                            "state": state,
                            "zip_code": zip_code,
                            "rating": rating,
                            "reviews": reviews,
                            "category": category,
                        }
                        count += 1
                        print(
                            f"    [{count}] {name} | {phone or 'N/A'} | {(website[:35] + '...') if website else 'N/A'}"
                        )
                        yield lead
                    except:
                        continue

                print(f"    Page {pg}: {count} leads")

                if not soup.select_one("a.next"):
                    print(f"    No next page.")
                    break

            except Exception as e:
                print(f"  YP error: {e}")
                break

        print(f"  [YP] Done.")

    # ── Yelp ──────────────────────────────────────────────────
    def search_yelp(self, query, location):
        """Scrapes Yelp for business listings."""
        if not self.page:
            self.start_browser()

        search_term = query.replace(" ", "+")
        loc_term = location.replace(", ", "+").replace(" ", "+")

        seen = set()

        for pg in range(0, 5):
            start = pg * 10
            url = f"https://www.yelp.com/search?find_desc={search_term}&find_loc={loc_term}&start={start}"
            print(f"  [Yelp] Page {pg + 1}: {url}")

            try:
                self._delay(2, 4)
                self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                self._delay(2, 4)

                content = self.page.content()
                if "captcha" in content.lower() or "unusual" in content.lower():
                    print(f"  [Yelp] Blocked on page {pg + 1}")
                    if self.headful:
                        time.sleep(20)
                    else:
                        break

                self._scroll(2)
                html = self.page.content()
                soup = BeautifulSoup(html, "html.parser")

                listings = soup.select('[data-testid="serp-ia-card"]') or soup.select(
                    "div.container__09f24__21w3G"
                )

                if not listings:
                    print(f"    No listings on page {pg + 1}.")
                    break

                print(f"    {len(listings)} listings")
                count = 0

                for listing in listings:
                    try:
                        name_el = listing.select_one(
                            'a[href*="/biz/"]'
                        ) or listing.select_one("h3 a")
                        if not name_el:
                            continue
                        name = name_el.get_text(strip=True)
                        if not name or len(name) < 2 or name.lower() in seen:
                            continue
                        seen.add(name.lower())

                        phone_el = listing.select_one(
                            '[class*="phone"]'
                        ) or listing.select_one('p:has(a[href^="tel:"])')
                        phone = ""
                        if phone_el:
                            phone = phone_el.get_text(strip=True)

                        address = ""
                        addr_el = listing.select_one("address") or listing.select_one(
                            '[class*="secondary"]'
                        )
                        if addr_el:
                            address = addr_el.get_text(strip=True)

                        category = ""
                        cat_els = listing.select(
                            '[class*="category"] a'
                        ) or listing.select('span:has(a[href*="/search?cflt="])')
                        if cat_els:
                            category = ", ".join(
                                [c.get_text(strip=True) for c in cat_els]
                            )

                        rating = ""
                        rating_el = listing.select_one('[aria-label*="star rating"]')
                        if rating_el:
                            m = re.search(
                                r"(\d+\.?\d*)\s*star", rating_el.get("aria-label", "")
                            )
                            if m:
                                rating = m.group(1)

                        reviews = ""
                        rev_el = listing.select_one('span:has(> a[href*="review"])')
                        if rev_el:
                            m = re.search(r"(\d+)", rev_el.get_text())
                            if m:
                                reviews = m.group(1)

                        biz_url = ""
                        if name_el.get("href"):
                            href = name_el["href"]
                            if href.startswith("/"):
                                biz_url = f"https://www.yelp.com{href}"
                            elif href.startswith("http"):
                                biz_url = href

                        city, state, zip_code = self._parse_address(address)

                        lead = {
                            "name": name,
                            "source": "Yelp",
                            "website": "",
                            "phone": phone,
                            "address": address,
                            "city": city,
                            "state": state,
                            "zip_code": zip_code,
                            "rating": rating,
                            "reviews": reviews,
                            "category": category,
                            "yelp_url": biz_url,
                        }
                        count += 1
                        print(
                            f"    [{count}] {name} | {phone or 'N/A'} | {address[:30] if address else 'N/A'}"
                        )
                        yield lead
                    except:
                        continue

                print(f"    Page {pg + 1}: {count} leads")

            except Exception as e:
                print(f"  Yelp error: {e}")
                break

        print(f"  [Yelp] Done.")

    def _yp_redirect(self, href):
        try:
            if href.startswith("/"):
                href = "https://www.yellowpages.com" + href
            p = urlparse(href)
            params = dict(x.split("=", 1) for x in p.query.split("&") if "=" in x)
            d = params.get("to") or params.get("url") or params.get("u")
            if d:
                decoded = urllib.parse.unquote(d)
                if "yellowpages" not in decoded.lower():
                    return decoded
        except:
            pass
        return None
