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
        # Ensure browsers are installed
        install_playwright()
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=not self.headful)
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/Chicago',
        )
        self.page = self.context.new_page()
        Stealth().apply_stealth_sync(self.page)
        self.enrich_page = self.context.new_page()
        Stealth().apply_stealth_sync(self.enrich_page)
        
    def close_browser(self):
        try:
            if self.browser: self.browser.close()
            if self.playwright: self.playwright.stop()
        except: pass

    def _delay(self, lo=1.0, hi=3.0):
        time.sleep(random.uniform(lo, hi))

    def _scroll(self, n=2):
        if not self.page: return
        for _ in range(n):
            try:
                self.page.mouse.move(random.randint(200, 900), random.randint(200, 500))
                self.page.evaluate(f"window.scrollBy(0, {random.randint(300, 600)})")
                time.sleep(random.uniform(0.5, 1.2))
            except: break

    # ── Extraction helpers ────────────────────────────────────
    def _find_emails(self, text):
        raw = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))
        bad = ('.png','.jpg','.jpeg','.gif','.webp','.svg','.css','.js')
        return [e for e in raw if not e.endswith(bad) and 'example' not in e and 'sentry' not in e]

    def _find_socials(self, html):
        soc = {'facebook': None, 'instagram': None, 'linkedin': None}
        if not html: return soc
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            h = a['href'].lower()
            if 'facebook.com/' in h and '/sharer' not in h and not soc['facebook']:
                soc['facebook'] = a['href']
            elif 'instagram.com/' in h and not soc['instagram']:
                soc['instagram'] = a['href']
            elif 'linkedin.com/' in h and '/shareArticle' not in h and not soc['linkedin']:
                soc['linkedin'] = a['href']
        return soc

    # ── Fast Enrichment (max 2 pages per site) ────────────────
    def enrich_website(self, base_url):
        enriched = {'email': None, 'facebook': None, 'instagram': None, 'linkedin': None}
        if not base_url or not base_url.startswith('http'):
            return enriched
        if not self.enrich_page:
            self.start_browser()

        parsed = urlparse(base_url)
        domain_base = f"{parsed.scheme}://{parsed.netloc}"
        
        # Only check homepage and /contact — fast!
        urls = [base_url, urljoin(domain_base, '/contact')]
        
        emails = set()
        socials = {'facebook': None, 'instagram': None, 'linkedin': None}
        
        for url in urls:
            try:
                self.enrich_page.goto(url, wait_until='domcontentloaded', timeout=8000)
                time.sleep(random.uniform(0.5, 1.5))
                html = self.enrich_page.content()
                
                emails.update(self._find_emails(html))
                
                ps = self._find_socials(html)
                for k, v in ps.items():
                    if v and not socials[k]: socials[k] = v
                
                # mailto: links
                soup = BeautifulSoup(html, 'html.parser')
                for a in soup.find_all('a', href=True):
                    if a['href'].startswith('mailto:'):
                        em = a['href'].replace('mailto:', '').split('?')[0].strip()
                        if '@' in em: emails.add(em)
                
                if emails and all(socials.values()):
                    break
            except:
                continue
        
        if emails:
            generic = ['info@', 'admin@', 'noreply@', 'no-reply@']
            specific = [e for e in emails if not any(e.lower().startswith(g) for g in generic)]
            enriched['email'] = specific[0] if specific else list(emails)[0]
        
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

        results = []
        try:
            self._delay(2, 4)
            self.page.goto(url, wait_until='domcontentloaded', timeout=40000)
            self._delay(3, 5)

            # Consent popup
            try:
                for text in ["Accept all", "I agree", "Agree"]:
                    btn = self.page.locator(f'button:has-text("{text}")')
                    if btn.count() > 0:
                        btn.first.click()
                        self._delay(2, 3)
                        print("    Cleared consent.")
                        break
            except: pass

            # Block check
            content = self.page.content()
            if "Pardon our interruption" in content or "unusual traffic" in content:
                print("  ⚠ Google CAPTCHA! Solve it in the browser.")
                if self.headful:
                    time.sleep(30)
                else:
                    return results

            # Scroll feed to load all listings
            try:
                feed = self.page.locator('div[role="feed"]')
                if feed.count() > 0:
                    for i in range(20):
                        feed.evaluate("node => node.scrollBy(0, 3000)")
                        time.sleep(random.uniform(0.8, 1.5))
                        # Check for end
                        try:
                            if self.page.locator('span.HlvSq').count() > 0:
                                print(f"    Reached end of feed after {i+1} scrolls")
                                break
                        except: pass
                        if (i+1) % 5 == 0:
                            print(f"    Scrolled {i+1} times...")
                else:
                    self._scroll(5)
            except: pass

            # Collect ALL listing links first
            all_links = self.page.locator('a[href*="/maps/place/"]').all()
            print(f"    Found {len(all_links)} place links")

            seen = set()
            link_data = []
            for link in all_links:
                try:
                    name = link.get_attribute('aria-label')
                    href = link.get_attribute('href') or ""
                    if not name or len(name.strip()) < 2: continue
                    name = name.strip()
                    if name.lower() in seen: continue
                    seen.add(name.lower())
                    link_data.append({'name': name, 'href': href, 'element': link})
                except: continue

            print(f"    {len(link_data)} unique businesses found")

            # Now click each listing to extract phone + website
            for i, item in enumerate(link_data):
                name = item['name']
                el = item['element']
                phone = ""
                website = ""

                try:
                    el.click(force=True)
                    self._delay(1.5, 3.0)

                    # Wait for detail panel
                    try:
                        self.page.wait_for_selector(
                            'button[data-tooltip="Copy phone number"], a[data-tooltip="Open website"], [data-item-id="phone"]',
                            timeout=4000
                        )
                    except: pass

                    # Phone — try multiple selectors
                    for sel in ['button[data-tooltip="Copy phone number"]', '[data-item-id="phone"] .Io6YTe', 'a[href^="tel:"]']:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                raw = el2.first.get_attribute('aria-label') or el2.first.inner_text() or ""
                                if 'tel:' in sel:
                                    raw = el2.first.get_attribute('href') or ""
                                    raw = raw.replace('tel:', '')
                                m = re.search(r'[\d()+.\-\s]{7,}', raw)
                                if m:
                                    phone = m.group().strip()
                                    break
                        except: continue

                    # Website — try multiple selectors
                    for sel in ['a[data-tooltip="Open website"]', '[data-item-id="authority"] a', 'a[aria-label*="website" i]']:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                website = el2.first.get_attribute('href') or ""
                                if website: break
                        except: continue

                    # Address, Category, Rating, Reviews
                    address = ""
                    category = ""
                    rating = ""
                    reviews = ""

                    for sel in ['button[data-tooltip="Copy address"]', '[data-item-id="address"]']:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                val = el2.first.get_attribute('aria-label') or el2.first.inner_text()
                                if val:
                                    address = val.replace('Address: ', '').replace('Copy address ', '').strip()
                                    break
                        except: pass

                    for sel in ['button[jsaction="pane.rating.category"]', '.fontBodyMedium.mgr77e']:
                        try:
                            el2 = self.page.locator(sel)
                            if el2.count() > 0:
                                category = el2.first.inner_text().strip()
                                break
                        except: pass
                        
                    try:
                        el2 = self.page.locator('.F7nice')
                        if el2.count() > 0:
                            txt = el2.first.inner_text()
                            parts = txt.split('\n')
                            if len(parts) >= 1: rating = parts[0].strip()
                            if len(parts) >= 2: reviews = parts[1].replace('(', '').replace(')', '').strip()
                    except: pass

                    # Hit Back to return to list
                    try:
                        back = self.page.locator('button[aria-label="Back"]')
                        if back.count() > 0:
                            back.first.click()
                            self._delay(0.8, 1.5)
                    except: pass

                except Exception as e:
                    # If clicking failed, try to recover by navigating back
                    try:
                        self.page.go_back()
                        self._delay(1.0, 2.0)
                    except: pass

                lead = {
                    'name': name,
                    'source': 'Google Maps',
                    'website': website.strip() if website else "",
                    'phone': phone.strip() if phone else "",
                    'address': address,
                    'rating': rating,
                    'reviews': reviews,
                    'category': category,
                }
                print(f"    [{i+1}/{len(link_data)}] {name} | {phone or 'N/A'} | {(website[:35] + '...') if website else 'N/A'}")
                yield lead

        except Exception as e:
            print(f"  Maps error: {e}")

        print(f"  [Maps] Done extraction.")

    # ── Yellow Pages (Multi-page) ─────────────────────────────
    def search_yellowpages(self, query, location):
        """Scrapes Yellow Pages across multiple pages."""
        if not self.page:
            self.start_browser()

        search_term = query.replace(" ", "+")
        loc_term = location.replace(", ", "+").replace(" ", "+")
        
        results = []
        seen = set()
        
        for pg in range(1, 6):
            url = f"https://www.yellowpages.com/search?search_terms={search_term}&geo_location_terms={loc_term}&page={pg}"
            print(f"  [YP] Page {pg}: {url}")

            try:
                self._delay(2, 4)
                self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
                self._delay(2, 4)
                
                content = self.page.content()
                if any(m in content.lower() for m in ["checking your browser", "access denied", "captcha"]):
                    print(f"  ⚠ YP blocked on page {pg}")
                    if self.headful:
                        print("  Waiting 20s for manual solve...")
                        time.sleep(20)
                    else:
                        break

                self._scroll(3)
                html = self.page.content()
                soup = BeautifulSoup(html, 'html.parser')

                listings = soup.select('div.result')
                if not listings:
                    listings = soup.select('div.v-card')
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
                            listing.select_one('a.business-name span') or
                            listing.select_one('a.business-name') or
                            listing.select_one('[class*="business-name"]') or
                            listing.select_one('h2 a')
                        )
                        if not name_el: continue
                        name = name_el.get_text(strip=True)
                        if not name or len(name) < 2: continue
                        if name.lower() in seen: continue
                        seen.add(name.lower())

                        phone = ""
                        ph = listing.select_one('div.phones') or listing.select_one('[class*="phone"]')
                        if ph: phone = ph.get_text(strip=True)

                        website = ""
                        for a in listing.select('a[href]'):
                            href = a.get('href', '')
                            cls = ' '.join(a.get('class', []))
                            
                            if 'track-visit-website' in cls:
                                if href.startswith('http') and 'yellowpages' not in href.lower():
                                    website = href; break
                                elif '/redirect?' in href:
                                    website = self._yp_redirect(href) or ""; 
                                    if website: break
                            
                            if '/redirect?' in href and not website:
                                r = self._yp_redirect(href)
                                if r: website = r; break

                        if not website:
                            for a in listing.select('a[href]'):
                                if a.get_text(strip=True).lower() in ('website', 'visit website'):
                                    href = a.get('href', '')
                                    if '/redirect?' in href:
                                        website = self._yp_redirect(href) or ""
                                    elif href.startswith('http') and 'yellowpages' not in href.lower():
                                        website = href
                                    if website: break

                        address = ""
                        adr_el = listing.select_one('.street-address')
                        loc_el = listing.select_one('.locality')
                        if adr_el: address += adr_el.get_text(strip=True)
                        if loc_el: address += ", " + loc_el.get_text(strip=True)

                        category = ""
                        cat_els = listing.select('.categories a')
                        if cat_els:
                            category = ", ".join([c.get_text(strip=True) for c in cat_els])

                        lead = {
                            'name': name,
                            'source': 'Yellow Pages',
                            'website': website,
                            'phone': phone,
                            'address': address.strip(', '),
                            'rating': "",
                            'reviews': "",
                            'category': category
                        }
                        count += 1
                        print(f"    [{count}] {name} | {phone or 'N/A'} | {(website[:35]+'...') if website else 'N/A'}")
                        yield lead
                    except: continue

                print(f"    Page {pg}: {count} new leads")
                
                if not soup.select_one('a.next'):
                    print(f"    No next page, stopping.")
                    break

            except Exception as e:
                print(f"  YP error: {e}")
                break

        print(f"  [YP] Done extraction.")

    def _yp_redirect(self, href):
        try:
            if href.startswith('/'): href = 'https://www.yellowpages.com' + href
            p = urlparse(href)
            params = dict(x.split('=', 1) for x in p.query.split('&') if '=' in x)
            d = params.get('to') or params.get('url') or params.get('u')
            if d:
                decoded = urllib.parse.unquote(d)
                if 'yellowpages' not in decoded.lower():
                    return decoded
        except: pass
        return None
