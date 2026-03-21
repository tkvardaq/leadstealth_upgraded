"""
Standalone CLI script to run the scraper as a separate process.
This avoids the Windows asyncio threading issue where Playwright's
sync_playwright() fails with NotImplementedError inside a thread.
"""
import argparse
import sys
import traceback
from scraper import LeadScraper
from processor import process_leads
from db import load_leads, save_leads


def main():
    parser = argparse.ArgumentParser(description="LeadStealth Scraper CLI")
    parser.add_argument("--query", required=True, help="Niche / Category")
    parser.add_argument("--location", required=True, help="Location")
    parser.add_argument("--headful", action="store_true", help="Show browser")
    args = parser.parse_args()

    leads_df = load_leads()
    scraper = LeadScraper(headful=args.headful)

    try:
        # ── Phase 1: Google Maps ──────────────────────────
        print("STATUS:Searching Google Maps...", flush=True)
        maps_generator = scraper.search_google_maps(args.query, args.location)
        print("STATUS:Extracting and enriching Google Maps leads...", flush=True)
        leads_df = process_leads(leads_df, maps_generator, scraper)

        # ── Phase 2: Yellow Pages (multi-page) ────────────
        print("STATUS:Searching Yellow Pages...", flush=True)
        yp_generator = scraper.search_yellowpages(args.query, args.location)
        print("STATUS:Extracting and enriching Yellow Pages leads...", flush=True)
        leads_df = process_leads(leads_df, yp_generator, scraper)

        total = len(leads_df)
        with_email = leads_df['email'].notna().sum() if 'email' in leads_df.columns else 0
        with_site = leads_df['website'].notna().sum() if 'website' in leads_df.columns else 0
        
        print(f"STATUS:Done — {total} leads, {with_email} emails, {with_site} websites", flush=True)

    except Exception as e:
        traceback.print_exc()
        print(f"ERROR:{e}", flush=True, file=sys.stderr)
    finally:
        scraper.close_browser()


if __name__ == "__main__":
    main()
