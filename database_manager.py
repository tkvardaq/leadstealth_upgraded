import os
import pandas as pd
from datetime import datetime

try:
    from supabase import create_client, Client

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False


class DatabaseManager:
    """
    Manages lead storage, switching between local CSV and Supabase cloud DB.
    """

    def __init__(self):
        self.use_supabase = False
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")

        if SUPABASE_AVAILABLE and self.supabase_url and self.supabase_key:
            try:
                self.client = create_client(self.supabase_url, self.supabase_key)
                self.use_supabase = True
                print("[DB] Using Supabase cloud storage.")
            except Exception as e:
                print(f"[DB] Supabase connection failed: {e}. Falling back to CSV.")
        else:
            print("[DB] Using local CSV storage.")

    def load_leads(self):
        """Load leads from the active storage"""
        expected_cols = [
            "id",
            "name",
            "source",
            "website",
            "phone",
            "email",
            "address",
            "city",
            "state",
            "zip_code",
            "category",
            "rating",
            "reviews",
            "facebook",
            "instagram",
            "linkedin",
            "twitter",
            "youtube",
            "hours",
            "description",
            "yelp_url",
            "campaign_status",
            "last_contact",
            "contact_count",
            "session_id",
            "session_name",
            "created_at",
            "updated_at",
        ]

        if self.use_supabase:
            try:
                response = self.client.table("leads").select("*").execute()
                df = pd.DataFrame(response.data)
                if df.empty:
                    return pd.DataFrame(columns=expected_cols)
                # Ensure all columns exist
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None
                return df
            except Exception as e:
                print(f"[DB] Supabase load error: {e}")
                return pd.DataFrame(columns=expected_cols)

        # Fallback to CSV
        csv_file = "leads.db.csv"
        if os.path.exists(csv_file):
            try:
                df = pd.read_csv(csv_file)
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None
                return df
            except:
                return pd.DataFrame(columns=expected_cols)
        return pd.DataFrame(columns=expected_cols)

    def save_leads(self, df):
        """Save leads to the active storage"""
        if self.use_supabase:
            try:
                # Supabase upsert requires a unique ID or primary key
                # Convert DF to list of dicts
                records = df.to_dict("records")
                # Clean up records (remove NaN which Supabase doesn't like)
                for rec in records:
                    for k, v in list(rec.items()):
                        if pd.isna(v):
                            rec[k] = None

                # We use 'name' and 'email' as a makeshift unique key if id is missing
                # But better to have a real ID.
                # For now, we'll try to upsert based on name if id is null
                self.client.table("leads").upsert(records).execute()
                return True
            except Exception as e:
                print(f"[DB] Supabase save error: {e}")
                # Don't return false, let it try to save to CSV as backup?

        # Save to CSV as primary or backup
        df.to_csv("leads.db.csv", index=False)
        return True

    def add_lead(self, lead_dict):
        """Add a single lead"""
        df = self.load_leads()
        new_row = pd.DataFrame([lead_dict])
        df = pd.concat([df, new_row], ignore_index=True)
        # Drop duplicates based on name/email
        if "email" in df.columns and not df["email"].isnull().all():
            df = df.drop_duplicates(subset=["name", "email"], keep="last")
        else:
            df = df.drop_duplicates(subset=["name"], keep="last")
        self.save_leads(df)
