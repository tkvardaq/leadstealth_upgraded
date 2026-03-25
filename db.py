import pandas as pd
import os
import json
from datetime import datetime
from pathlib import Path

# Use absolute path based on project root so it works regardless of CWD
_PROJECT_DIR = Path(__file__).parent.resolve()
CSV_FILE = str(_PROJECT_DIR / "leads.db.csv")
SESSIONS_FILE = str(_PROJECT_DIR / "sessions.db.json")

COLUMNS = [
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


def load_leads():
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df
    return pd.DataFrame(columns=COLUMNS)


def save_leads(df):
    if df is not None:
        df.to_csv(CSV_FILE, index=False)


def check_business_exists(df, name, website):
    if df.empty:
        return False
    name_match = df["name"].str.lower() == str(name).lower() if name else False
    website_match = df["website"] == website if website else False
    return df[name_match | website_match].shape[0] > 0


def has_email(df, name, website):
    if df.empty:
        return False
    name_match = df["name"].str.lower() == str(name).lower() if name else False
    website_match = df["website"] == website if website else False
    match = df[name_match | website_match]
    if not match.empty:
        email = match.iloc[0].get("email", None)
        return pd.notna(email) and str(email).strip() != ""
    return False


def add_or_update_lead(df, lead_data):
    now = datetime.now().isoformat()
    if df.columns.empty:
        df = pd.DataFrame(columns=COLUMNS)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None

    name = lead_data.get("name")
    website = lead_data.get("website")

    match_mask = pd.Series([False] * len(df))
    if not df.empty:
        if name:
            match_mask |= df["name"].str.lower() == str(name).lower()
        if website:
            match_mask |= df["website"] == website

    match_idx = df.index[match_mask].tolist()

    if match_idx:
        idx = match_idx[0]
        for key in COLUMNS:
            new_val = lead_data.get(key)
            if pd.notna(new_val) and str(new_val).strip() != "":
                df.at[idx, key] = new_val
        df.at[idx, "updated_at"] = now
    else:
        new_row = {col: lead_data.get(col) for col in COLUMNS}
        new_id = df["id"].max() + 1 if not df.empty and pd.notna(df["id"].max()) else 1
        new_row["id"] = new_id
        new_row["created_at"] = now
        new_row["updated_at"] = now
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    return df


# ── Session / Campaign History ───────────────────────────────


def load_sessions():
    """Load all past campaign sessions."""
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


def save_session(session_data):
    """Append a new campaign session to history."""
    sessions = load_sessions()
    sessions.append(session_data)
    with open(SESSIONS_FILE, "w") as f:
        json.dump(sessions, f, indent=2, default=str)


def get_sessions_df():
    """Return sessions as a DataFrame for display."""
    sessions = load_sessions()
    if not sessions:
        return pd.DataFrame(
            columns=[
                "session_id",
                "session_name",
                "date",
                "sources",
                "leads_found",
                "emails_found",
                "websites_found",
                "emails_sent",
                "emails_failed",
                "query",
                "location",
            ]
        )
    return pd.DataFrame(sessions)
