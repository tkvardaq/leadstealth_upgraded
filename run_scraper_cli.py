"""
Standalone CLI script to run the scraper as a separate process.
"""

import argparse
import sys
import traceback
from datetime import datetime
from scraper import LeadScraper
from processor import process_leads
from db import load_leads, save_leads, save_session


def main():
    parser = argparse.ArgumentParser(description="LeadStealth Scraper CLI")
    parser.add_argument("--query", required=True, help="Niche / Category")
    parser.add_argument("--location", required=True, help="Location")
    parser.add_argument("--sources", default="google_maps,yellowpages,yelp")
    parser.add_argument("--headful", action="store_true")
    args = parser.parse_args()

    leads_before = load_leads()
    count_before = len(leads_before)
    sources = [s.strip().lower() for s in args.sources.split(",")]

    try:
        leads_df = leads_before

        for source in sources:
            scraper = LeadScraper(headful=args.headful)
            try:
                if source == "google_maps":
                    print("STATUS:Searching Google Maps...", flush=True)
                    leads_df = process_leads(
                        leads_df,
                        scraper.search_google_maps(args.query, args.location),
                        scraper,
                    )
                elif source == "yellowpages":
                    print("STATUS:Searching Yellow Pages...", flush=True)
                    leads_df = process_leads(
                        leads_df,
                        scraper.search_yellowpages(args.query, args.location),
                        scraper,
                    )
                elif source == "yelp":
                    print("STATUS:Searching Yelp...", flush=True)
                    leads_df = process_leads(
                        leads_df,
                        scraper.search_yelp(args.query, args.location),
                        scraper,
                    )
                else:
                    print(f"WARNING: Unknown source '{source}' skipped", flush=True)
            finally:
                scraper.close_browser()

        total = len(leads_df)
        new_leads = total - count_before
        with_email = (
            int(leads_df["email"].notna().sum()) if "email" in leads_df.columns else 0
        )
        with_site = (
            int(leads_df["website"].notna().sum())
            if "website" in leads_df.columns
            else 0
        )

        # Save session history
        save_session(
            {
                "session_id": datetime.now().strftime("%Y%m%d%H%M%S"),
                "session_name": f"{args.query} in {args.location}",
                "date": datetime.now().isoformat(),
                "query": args.query,
                "location": args.location,
                "sources": ", ".join(sources),
                "leads_found": new_leads,
                "total_leads": total,
                "emails_found": with_email,
                "websites_found": with_site,
            }
        )

        print(
            f"STATUS:Done — {new_leads} new leads ({total} total), {with_email} emails",
            flush=True,
        )

    except Exception as e:
        traceback.print_exc()
        print(f"ERROR:{e}", flush=True, file=sys.stderr)


if __name__ == "__main__":
    main()
