from database.service import service as db

async def process_leads(leads_df, leads_generator, scraper):
    """Enriches leads as they stream in from the async generator. Saves to SQLite after each lead."""
    enriched_count = 0
    total = 0
    
    async for lead in leads_generator:
        total += 1
        name = lead.get('name', 'Unknown')
        website = lead.get('website', '')
        
        # Enrich from website
        if website and website.startswith('http'):
            try:
                enriched = await scraper.enrich_website(website)
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
        
        # Save to SQLite immediately via DatabaseService
        db.add_or_update_lead(lead)
    
    print(f"  Enrichment done: {enriched_count}/{total} emails found")
    return db.load_leads()
