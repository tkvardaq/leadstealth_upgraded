"""
LeadStealth Webmail Edition - Complete lead scraper + webmail sender
No SMTP costs - uses your HostFast hosting webmail
"""

import streamlit as st
import pandas as pd
import asyncio
import subprocess
import sys
import os
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent))

is_cloud = (
    os.environ.get("STREAMLIT_SERVER_PORT") is not None
    or os.environ.get("STREAMLIT_CLOUD_ID") is not None
)

from webmail.sender import WebmailSender, WebmailAccount, WebmailTemplateLibrary


def check_browser_install():
    """Ensure browsers are installed. Returns True if ready."""
    marker_file = Path(tempfile.gettempdir()) / ".playwright_installed"
    if is_cloud:
        if not marker_file.exists():
            # We will show this in the UI when the user tries to run a task
            return False
        return True
    return True


# ── Database Functions ─────────────────────────────────────────

from database_manager import DatabaseManager
from db import get_sessions_df, save_session


@st.cache_resource
def get_db():
    return DatabaseManager()


# ── Initialization ───────────────────────────────────────────
if "db_initialized" not in st.session_state:
    st.session_state.db_initialized = False
    st.session_state.browsers_installed = check_browser_install()

# ── Sidebar Setup ─────────────────────────────────────────────
with st.sidebar:
    st.title("🛡️ LeadStealth")
    st.write("Webmail Edition v2.1")

    if is_cloud:
        if not st.session_state.browsers_installed:
            if st.button(
                "🚀 Complete First-Time Setup",
                help="Installs browser engines (approx 1 min)",
            ):
                with st.status("🔧 Preparing browser engines..."):
                    from utils import install_playwright

                    install_playwright()
                    st.session_state.browsers_installed = True
                    st.success("✅ Setup complete! You can now start.")
                    st.rerun()
        else:
            st.success("✅ Browser Engines Ready")

    st.divider()

    # Progress UI
    if not st.session_state.db_initialized:
        with st.status("📦 Connecting to storage..."):
            db = get_db()
            st.session_state.db_initialized = True
            st.rerun()
    else:
        db = get_db()


def load_leads():
    """Load leads from DB manager"""
    return db.load_leads()


def save_leads(df):
    """Save leads to DB manager"""
    db.save_leads(df)


# ── Initialization ───────────────────────────────────────────

# ── Session State ────────────────────────────────────────────
if "scanning" not in st.session_state:
    st.session_state.scanning = False
if "sending" not in st.session_state:
    st.session_state.sending = False
if "leads_df" not in st.session_state:
    st.session_state.leads_df = load_leads()
if "process" not in st.session_state:
    st.session_state.process = None
if "status_msg" not in st.session_state:
    st.session_state.status_msg = ""
if "webmail_accounts" not in st.session_state:
    st.session_state.webmail_accounts = []
if "campaign_stats" not in st.session_state:
    st.session_state.campaign_stats = None


# ── Sidebar: Webmail Configuration ──────────────────────────

with st.sidebar:
    st.header("📧 Webmail Setup")
    st.info("Use your HostFast hosting webmail - it's FREE!")

    with st.expander(
        "➕ Add Webmail Account", expanded=len(st.session_state.webmail_accounts) == 0
    ):
        with st.form("add_account"):
            st.caption("Your HostFast webmail credentials")

            wm_name = st.text_input("Your Name", placeholder="John Smith")
            wm_email = st.text_input("Your Email", placeholder="you@yourdomain.com")
            wm_pass = st.text_input("Webmail Password", type="password")
            wm_url = st.text_input(
                "Webmail URL",
                value="https://webmail.yourdomain.com",
                help="Usually: https://webmail.yourdomain.com or https://yourdomain.com:2096",
            )

            col1, col2 = st.columns(2)
            with col1:
                wm_daily = st.number_input(
                    "Daily Limit", value=100, min_value=10, max_value=500
                )
            with col2:
                wm_hourly = st.number_input(
                    "Hourly Limit", value=15, min_value=5, max_value=50
                )

            submitted = st.form_submit_button("Add Account", type="primary")

            if submitted and wm_email and wm_pass:
                account = WebmailAccount(
                    name=wm_name,
                    email=wm_email,
                    password=wm_pass,
                    webmail_url=wm_url,
                    daily_limit=wm_daily,
                    hourly_limit=wm_hourly,
                )
                st.session_state.webmail_accounts.append(account)
                st.success(f"✓ Added {wm_email}")

    # Show configured accounts
    if st.session_state.webmail_accounts:
        st.subheader("📧 Configured Accounts")
        for i, acc in enumerate(st.session_state.webmail_accounts):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.caption(f"📧 {acc.email}")
                st.caption(f"Daily: {acc.daily_limit} | Hourly: {acc.hourly_limit}")
            with col2:
                if st.button("🗑️", key=f"del_{i}"):
                    st.session_state.webmail_accounts.pop(i)
                    st.rerun()


# ── Header ───────────────────────────────────────────────────

st.title("🕵️ LeadStealth Webmail Edition")
st.caption("Scrape leads from Google Maps, Yellow Pages & Yelp → Send via FREE webmail")

# ── Main Tabs ────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["🔍 Lead Scraping", "📧 Email Campaigns", "📊 Analytics"])


# ── Tab 1: Lead Scraping ─────────────────────────────────────

with tab1:
    st.header("Find New Leads")

    col1, col2 = st.columns(2)
    with col1:
        query = st.text_input(
            "Business Category",
            value="Plumbers",
            help="e.g., Plumbers, Electricians, HVAC, Dentists",
        )
    with col2:
        location = st.text_input(
            "Location", value="Austin, TX", help="City, State or full address"
        )

    if not db.use_supabase:
        st.warning(
            "⚠️ **Warning: Local Storage Active**\n\nData will be lost when the app sleeps or restarts on Streamlit Cloud. Connect **Supabase** for permanent storage."
        )
        with st.expander("🛠️ How to connect Supabase (Fix Data Loss)"):
            st.markdown("""
            1. Create a free project at [supabase.com](https://supabase.com)
            2. Run the SQL script I provided in their **SQL Editor**.
            3. Go to **Settings > API** to get your URL and Anon Key.
            4. In Streamlit Cloud, go to **Settings > Secrets** and add:
            ```toml
            SUPABASE_URL = "your-url"
            SUPABASE_KEY = "your-anon-key"
            ```
            """)
    else:
        st.success("☁️ **Supabase Connected**: Your data is safe and persistent!")

    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        sources = st.multiselect(
            "Sources",
            ["Google Maps", "Yellow Pages", "Yelp"],
            default=["Google Maps"],
            help="Where to search for leads",
        )
    with col2:
        max_leads = st.number_input(
            "Max Leads",
            value=50,
            min_value=10,
            max_value=500,
            help="Stop after collecting this many leads",
        )
    with col3:
        # Detect cloud to force headless
        is_cloud = (
            os.environ.get("STREAMLIT_SERVER_PORT") is not None
            or os.environ.get("STREAMLIT_CLOUD_ID") is not None
        )
        if is_cloud:
            headful = st.checkbox(
                "Show Browser (Local Only)",
                value=False,
                disabled=True,
                help="Visible browser windows are NOT supported in the cloud. Headless mode forced.",
            )
            headful = False  # Force false regardless of checkbox if on cloud
        else:
            headful = st.checkbox(
                "Show Browser",
                value=True,
                help="Keep browser visible to solve CAPTCHAs manually (Local only)",
            )

    # Start scraping
    if st.session_state.scanning:
        st.warning("⏳ Scraping in progress...")
        progress_bar = st.progress(0)

        if st.button("🛑 Stop Scraping"):
            if st.session_state.process:
                st.session_state.process.terminate()
            st.session_state.scanning = False
            st.session_state.process = None
            st.rerun()
    else:
        if st.button("🚀 Start Scraping", type="primary"):
            # Build source list for CLI
            source_map = {
                "Google Maps": "google_maps",
                "Yellow Pages": "yellowpages",
                "Yelp": "yelp",
            }
            source_args = ",".join(source_map[s] for s in sources if s in source_map)

            cmd = [
                sys.executable,
                "run_scraper_cli.py",
                "--query",
                query,
                "--location",
                location,
                "--sources",
                source_args,
            ]
            if headful:
                cmd.append("--headful")

            st.session_state.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
            st.session_state.scanning = True
            st.rerun()

    # Check process status
    if st.session_state.scanning and st.session_state.process:
        retcode = st.session_state.process.poll()
        st.session_state.leads_df = load_leads()

        if retcode is not None:
            st.session_state.scanning = False
            stdout, stderr = st.session_state.process.communicate()

            # Parse status
            for line in stdout.strip().splitlines():
                if line.startswith("STATUS:"):
                    st.session_state.status_msg = line.replace("STATUS:", "")

            if stderr.strip():
                st.error(f"Error: {stderr.strip()[:200]}")
            else:
                st.success(
                    f"✓ Scraping complete! {len(st.session_state.leads_df)} total leads in database."
                )

            st.session_state.process = None

    if st.session_state.status_msg:
        st.info(st.session_state.status_msg)


# ── Tab 2: Email Campaigns ───────────────────────────────────

with tab2:
    st.header("Send Email Campaigns")

    if not st.session_state.webmail_accounts:
        st.error("⚠️ Please add at least one webmail account in the sidebar first!")
    elif st.session_state.leads_df.empty:
        st.warning("No leads found. Scrape some leads first!")
    else:
        # Load templates
        templates = WebmailTemplateLibrary.get_templates()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🎯 Target Leads")

            # Filter options
            has_email = st.checkbox("Only leads with email", value=True)

            if has_email:
                leads_with_email = st.session_state.leads_df[
                    st.session_state.leads_df["email"].notna()
                    & (st.session_state.leads_df["email"] != "")
                ]
            else:
                leads_with_email = st.session_state.leads_df

            # --- Added Location & Category filters ---
            if "address" in leads_with_email.columns:
                locations = sorted(
                    [
                        str(x)
                        for x in leads_with_email["address"].dropna().unique()
                        if str(x).strip()
                    ]
                )
                loc_filter = st.multiselect("Filter by Location", locations)
                if loc_filter:
                    leads_with_email = leads_with_email[
                        leads_with_email["address"].isin(loc_filter)
                    ]

            if "category" in leads_with_email.columns:
                categories = []
                for cat in leads_with_email["category"].dropna():
                    categories.extend([c.strip() for c in str(cat).split(",")])
                categories = sorted(list(set([c for c in categories if c])))
                cat_filter = st.multiselect("Filter by Category", categories)
                if cat_filter:
                    leads_with_email = leads_with_email[
                        leads_with_email["category"].apply(
                            lambda x: any(c in str(x) for c in cat_filter)
                        )
                    ]

            # Filter by status
            status_filter = st.multiselect(
                "Campaign Status",
                ["new", "contacted", "responded", "bounced"],
                default=["new"],
                help="Only email leads with these statuses",
            )

            if status_filter and "campaign_status" in leads_with_email.columns:
                leads_filtered = leads_with_email[
                    leads_with_email["campaign_status"].isin(status_filter)
                    | leads_with_email["campaign_status"].isna()
                ]
            else:
                leads_filtered = leads_with_email

            # Limit selection
            max_to_send = st.slider(
                "Max emails to send",
                min_value=1,
                max_value=min(len(leads_filtered), 100),
                value=min(10, len(leads_filtered)),
            )

            st.metric("Leads selected", len(leads_filtered))

            # Preview
            if not leads_filtered.empty:
                st.caption("Preview:")
                preview_df = leads_filtered[["name", "email", "website"]].head(5)
                st.dataframe(preview_df)

        with col2:
            st.subheader("✉️ Email Template")

            template_choice = st.selectbox(
                "Choose Template",
                list(templates.keys()),
                format_func=lambda x: x.replace("_", " ").title(),
            )

            selected_template = templates[template_choice]

            # Show/edit subject
            subject = st.text_input("Subject", value=selected_template["subject"])

            # Show body
            with st.expander("Edit Email Body"):
                body = st.text_area("Body", value=selected_template["body"], height=300)

            # Personalization preview
            st.caption(
                "💡 Available variables: {{ company }}, {{ first_name }}, {{ location }}, {{ sender_name }}"
            )

            # Test on first lead
            if not leads_filtered.empty:
                test_lead = leads_filtered.iloc[0]
                from jinja2 import Template

                test_subj = Template(subject).render(
                    company=test_lead["name"],
                    first_name=test_lead["name"].split()[0]
                    if " " in test_lead["name"]
                    else test_lead["name"],
                )
                st.caption(f"Preview subject: *{test_subj}*")

        # Campaign settings
        st.divider()
        st.subheader("⚙️ Campaign Settings")

        session_default = f"Campaign {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        session_name = st.text_input(
            "Session / Campaign Name",
            value=session_default,
            help="Identify this campaign session to track follow-ups",
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            delay_min = st.number_input("Min delay (seconds)", value=90, min_value=30)
        with col2:
            delay_max = st.number_input("Max delay (seconds)", value=180, min_value=60)
        with col3:
            st.info(
                f"Sending ~{max_to_send} emails with {delay_min}-{delay_max}s delays"
            )

        # Send button
        if st.session_state.sending:
            st.warning("⏳ Campaign running...")
            if st.button("🛑 Stop Campaign"):
                st.session_state.sending = False
                st.rerun()
        else:
            if st.button("🚀 Start Campaign", type="primary"):
                if len(leads_filtered) == 0:
                    st.error("No leads match your filters!")
                elif len(st.session_state.webmail_accounts) == 0:
                    st.error("Add a webmail account first!")
                else:
                    st.session_state.sending = True
                    st.rerun()

        # Campaign execution
        if st.session_state.sending:
            with st.spinner("Sending emails via webmail..."):
                progress = st.progress(0)
                status_text = st.empty()

                async def run_campaign():
                    # Set headless based on the UI checkbox (forced False on Cloud)
                    sender = WebmailSender(headless=not headful)
                    await sender.start()

                    # Add all accounts
                    for acc in st.session_state.webmail_accounts:
                        sender.add_account(acc)

                    # Prepare recipients
                    recipients = []
                    for _, row in leads_filtered.head(max_to_send).iterrows():
                        first_name = (
                            row["name"].split()[0]
                            if " " in str(row["name"])
                            else str(row["name"])
                        )
                        recipients.append(
                            {
                                "email": row["email"],
                                "company": row["name"],
                                "first_name": first_name,
                                "location": location,
                                "sender_name": st.session_state.webmail_accounts[
                                    0
                                ].name,
                                "sender_company": "Digital Marketing Agency",
                                "sender_email": st.session_state.webmail_accounts[
                                    0
                                ].email,
                            }
                        )

                    results = await sender.send_campaign(
                        recipients=recipients,
                        subject_template=subject,
                        body_template=body,
                        delay_range=(delay_min, delay_max),
                    )

                    await sender.close()
                    return results

                # Run async
                try:
                    if sys.platform == "win32":
                        loop = asyncio.ProactorEventLoop()
                        asyncio.set_event_loop(loop)
                        results = loop.run_until_complete(run_campaign())
                        loop.close()
                    else:
                        results = asyncio.run(run_campaign())
                    st.session_state.campaign_stats = results
                    st.session_state.sending = False

                    # Update lead statuses
                    df = st.session_state.leads_df.copy()
                    current_session_id = datetime.now().strftime("%Y%m%d%H%M%S")
                    for i, (_, row) in enumerate(
                        leads_filtered.head(max_to_send).iterrows()
                    ):
                        df.loc[df.index == row.name, "campaign_status"] = "Sent"
                        df.loc[df.index == row.name, "last_contact"] = (
                            datetime.now().isoformat()
                        )
                        df.loc[df.index == row.name, "session_id"] = current_session_id
                        df.loc[df.index == row.name, "session_name"] = session_name
                        count = df.loc[df.index == row.name, "contact_count"].values[0]
                        df.loc[df.index == row.name, "contact_count"] = (
                            1 if pd.isna(count) else count + 1
                        )
                    save_leads(df)
                    st.session_state.leads_df = df

                    # Save campaign session to history
                    save_session(
                        {
                            "session_id": current_session_id,
                            "session_name": session_name,
                            "date": datetime.now().isoformat(),
                            "type": "email_campaign",
                            "emails_sent": results.get("sent", 0),
                            "emails_failed": results.get("failed", 0),
                            "total_recipients": results.get("total", 0),
                        }
                    )

                    st.rerun()

                except Exception as e:
                    st.error(f"Campaign error: {e}")
                    st.session_state.sending = False

        # Show results
        if st.session_state.campaign_stats:
            stats = st.session_state.campaign_stats
            st.divider()
            st.subheader("📊 Campaign Results")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total", stats["total"])
            with col2:
                st.metric("Sent", stats["sent"], delta=f"+{stats['sent']}")
            with col3:
                st.metric("Failed", stats["failed"])

            if stats["errors"]:
                with st.expander("❌ Errors"):
                    for error in stats["errors"][:10]:
                        st.error(error)


# ── Tab 3: Analytics ─────────────────────────────────────────

with tab3:
    st.header("📊 Lead Database")

    df = st.session_state.leads_df

    if df.empty:
        st.info("No leads yet. Go to the Lead Scraping tab to find some!")
    else:
        # Stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Leads", len(df))
        with col2:
            has_email = df["email"].notna().sum() if "email" in df.columns else 0
            st.metric("With Email", has_email)
        with col3:
            has_website = df["website"].notna().sum() if "website" in df.columns else 0
            st.metric("With Website", has_website)
        with col4:
            if "campaign_status" in df.columns:
                contacted = df[df["campaign_status"].isin(["contacted", "Sent"])].shape[
                    0
                ]
                st.metric("Sent/Contacted", contacted)

        # Charts
        st.divider()
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        with chart_col1:
            if "campaign_status" in df.columns:
                status_counts = df["campaign_status"].fillna("new").value_counts()
                st.subheader("Campaign Status")
                st.bar_chart(status_counts)
        with chart_col2:
            if "category" in df.columns:
                cats = df["category"].dropna().str.split(",").explode().str.strip()
                cat_counts = cats[cats != ""].value_counts().head(8)
                if not cat_counts.empty:
                    st.subheader("Top Categories")
                    st.bar_chart(cat_counts)
        with chart_col3:
            if "state" in df.columns:
                state_counts = df["state"].dropna()
                state_counts = state_counts[state_counts != ""].value_counts().head(8)
                if not state_counts.empty:
                    st.subheader("Leads by State")
                    st.bar_chart(state_counts)

        # Filter display
        st.divider()

        analytics_tabs = st.tabs(
            [
                "📋 All Leads",
                "✉️ Contacted Leads (Sessions)",
                "📅 Follow-Ups",
                "🕐 Scrape History",
            ]
        )

        with analytics_tabs[0]:
            col1, col2 = st.columns([1, 4])
            with col1:
                show_missing = st.checkbox(
                    "Only missing email", value=False, key="chk_missing"
                )

            display_df = df.copy()
            if show_missing and "email" in df.columns:
                display_df = display_df[
                    display_df["email"].isna() | (display_df["email"] == "")
                ]

            st.dataframe(
                display_df,
                column_config={
                    "website": st.column_config.LinkColumn("Website"),
                    "email": st.column_config.LinkColumn("Email"),
                    "facebook": st.column_config.LinkColumn("Facebook"),
                    "instagram": st.column_config.LinkColumn("Instagram"),
                    "linkedin": st.column_config.LinkColumn("LinkedIn"),
                    "twitter": st.column_config.LinkColumn("Twitter"),
                    "yelp_url": st.column_config.LinkColumn("Yelp"),
                    "campaign_status": st.column_config.TextColumn(
                        "Status", help="Campaign Status"
                    ),
                    "rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                    "reviews": st.column_config.NumberColumn("Reviews"),
                    "city": st.column_config.TextColumn("City"),
                    "state": st.column_config.TextColumn("State"),
                    "zip_code": st.column_config.TextColumn("Zip"),
                    "category": st.column_config.TextColumn("Category"),
                    "hours": st.column_config.TextColumn("Hours"),
                },
            )

        with analytics_tabs[1]:
            st.subheader("Contacted Leads by Session")
            if "session_id" in df.columns and not df[df["session_id"].notna()].empty:
                contacted_df = df[df["session_id"].notna()].copy()
                sessions = (
                    contacted_df[["session_id", "session_name", "last_contact"]]
                    .drop_duplicates(subset=["session_id"])
                    .sort_values("last_contact", ascending=False)
                )

                selected_session_name = st.selectbox(
                    "Select Campaign Session",
                    ["All Sessions"] + sessions["session_name"].tolist(),
                )

                if selected_session_name != "All Sessions":
                    session_id = sessions[
                        sessions["session_name"] == selected_session_name
                    ].iloc[0]["session_id"]
                    contacted_df = contacted_df[
                        contacted_df["session_id"] == session_id
                    ]

                st.dataframe(
                    contacted_df,
                    column_config={
                        "website": st.column_config.LinkColumn("Website"),
                        "email": st.column_config.LinkColumn("Email"),
                        "campaign_status": st.column_config.TextColumn(
                            "Status", default="Sent 🟢"
                        ),
                    },
                )
            else:
                st.info("No completed campaigns with session tracking yet.")

        with analytics_tabs[2]:
            st.subheader("Follow-Ups")
            st.caption("Leads contacted more than 3 days ago without a response.")
            if "last_contact" in df.columns and "campaign_status" in df.columns:
                followup_df = df[
                    (df["campaign_status"].isin(["Sent", "contacted"]))
                    & (df["last_contact"].notna())
                ].copy()

                if not followup_df.empty:
                    now = datetime.now()
                    followup_df["days_since_contact"] = followup_df[
                        "last_contact"
                    ].apply(
                        lambda x: (
                            (now - datetime.fromisoformat(x)).days if pd.notna(x) else 0
                        )
                    )
                    followup_df = followup_df[
                        followup_df["days_since_contact"] >= 3
                    ].sort_values("days_since_contact", ascending=False)

                    st.dataframe(
                        followup_df,
                        column_config={
                            "website": st.column_config.LinkColumn("Website"),
                            "email": st.column_config.LinkColumn("Email"),
                            "days_since_contact": st.column_config.NumberColumn(
                                "Days Since Contact", format="%d days"
                            ),
                        },
                    )
                else:
                    st.info("No leads currently require follow up.")
            else:
                st.info("No contact data available yet.")

        with analytics_tabs[3]:
            st.subheader("Scrape Session History")
            sessions_df = get_sessions_df()
            if sessions_df.empty:
                st.info(
                    "No scrape sessions recorded yet. Run a scrape to see history here."
                )
            else:
                st.dataframe(
                    sessions_df,
                    column_config={
                        "date": st.column_config.DatetimeColumn("Date"),
                        "leads_found": st.column_config.NumberColumn("New Leads"),
                        "total_leads": st.column_config.NumberColumn("Total Leads"),
                        "emails_found": st.column_config.NumberColumn("Emails Found"),
                        "websites_found": st.column_config.NumberColumn("Websites"),
                    },
                    use_container_width=True,
                )
                st.caption(f"Total sessions: {len(sessions_df)}")

        # Export
        st.divider()
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Full Database CSV", csv, "leads_export.csv", "text/csv"
        )


# ── Auto-refresh while scanning ─────────────────────────────

if st.session_state.scanning:
    time.sleep(3)
    st.rerun()

if st.session_state.sending:
    time.sleep(2)
    st.rerun()
