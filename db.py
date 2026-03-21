import pandas as pd
import os
from datetime import datetime

CSV_FILE = 'leads.db.csv' # using .db.csv to denote its role as our database

COLUMNS = [
    'id', 'name', 'source', 'website', 'phone', 'email',
    'facebook', 'instagram', 'linkedin',
    'pagespeed_mobile', 'pagespeed_desktop',
    'created_at', 'updated_at'
]

def load_leads():
    """Loads leads from CSV if it exists, otherwise returns an empty DataFrame."""
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        # Ensure all expected columns exist
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df
    else:
        return pd.DataFrame(columns=COLUMNS)

def save_leads(df):
    """Saves the DataFrame to CSV."""
    if df is not None:
        print(f"  Saving {len(df)} leads to {CSV_FILE}")
        df.to_csv(CSV_FILE, index=False)
    else:
        print("  WARNING: Attempted to save None as leads.")

def check_business_exists(df, name, website):
    """Checks if a business already exists based on name or website."""
    if df.empty:
        return False
    
    # Check by name (case-insensitive) or website
    name_match = df['name'].str.lower() == str(name).lower() if name else False
    website_match = df['website'] == website if website else False
    
    return df[name_match | website_match].shape[0] > 0

def has_email(df, name, website):
    """Checks if an existing business has an email address."""
    if df.empty:
        return False
        
    name_match = df['name'].str.lower() == str(name).lower() if name else False
    website_match = df['website'] == website if website else False
    
    match = df[name_match | website_match]
    if not match.empty:
        if 'email' not in df.columns:
            return False
        email = match.iloc[0].get('email', None)
        return pd.notna(email) and str(email).strip() != ""
    return False

def add_or_update_lead(df, lead_data):
    """Adds a new lead or updates an existing one if it was missing info."""
    now = datetime.now().isoformat()
    
    # Ensure DataFrame has columns
    if df.columns.empty or not all(col in df.columns for col in COLUMNS):
        df = pd.DataFrame(columns=COLUMNS)

    name = lead_data.get('name')
    website = lead_data.get('website')
    
    # Find match
    match_mask = pd.Series([False] * len(df))
    if not df.empty:
        if name:
            match_mask |= (df['name'].str.lower() == str(name).lower())
        if website:
            match_mask |= (df['website'] == website)
    
    match_idx = df.index[match_mask].tolist()

    if match_idx:
        # Update existing
        idx = match_idx[0]
        # Only update missing or improved info
        for key in COLUMNS:
            new_val = lead_data.get(key)
            if pd.notna(new_val) and str(new_val).strip() != "":
                # Don't overwrite existing email if new is empty
                df.at[idx, key] = new_val
        df.at[idx, 'updated_at'] = now
    else:
        # Add new
        new_row_data = {col: lead_data.get(col) for col in COLUMNS}
        new_id = df['id'].max() + 1 if not df.empty and not pd.isna(df['id'].max()) else 1
        new_row_data['id'] = new_id
        new_row_data['created_at'] = now
        new_row_data['updated_at'] = now
        
        df = pd.concat([df, pd.DataFrame([new_row_data])], ignore_index=True)
        
    return df
