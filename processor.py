import db

def process_leads(leads_df, leads_generator, scraper):
    """Enriches leads as they stream in from the generator. Saves after each lead."""
    enriched_count = 0
    total = 0
    
    for lead in leads_generator:
        total += 1
        name = lead.get('name', 'Unknown')
        website = lead.get('website', '')
        
        # Skip if already fully enriched in DB
        exists = db.check_business_exists(leads_df, name, website)
        if exists and db.has_email(leads_df, name, website):
            print(f"  [{total}] Skip {name} — has email")
            # Save anyway to update Streamlit UI if it's the first few
            if total % 3 == 0:
                db.save_leads(leads_df)
            continue
        
        # Enrich from website
        if website and website.startswith('http'):
            try:
                enriched = scraper.enrich_website(website)
                lead.update(enriched)
                if enriched.get('email'):
                    enriched_count += 1
                    print(f"  [{total}] ✓ {name} — email: {enriched['email']}")
                else:
                    print(f"  [{total}] {name} — no email found")
            except Exception as e:
                print(f"  [{total}] {name} — enrichment error: {e}")
        else:
            print(f"  [{total}] {name} — no website to enrich")
        
        # Save to DataFrame and disk immediately
        leads_df = db.add_or_update_lead(leads_df, lead)
        db.save_leads(leads_df)
    
    print(f"  Enrichment done: {enriched_count}/{total} emails found")
    return leads_df
