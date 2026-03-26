"""
LeadStealth Webmail Edition - Complete lead scraper + webmail sender
No SMTP costs - uses your HostFast hosting webmail
"""

import streamlit as st
import pandas as pd
import os
import sys
import json
import time
import asyncio
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

# Windows-specific fix for Playwright/Subprocess NotImplementedError
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

st.set_page_config(
    page_title="LeadStealth CRM",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

CRM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Inter:wght@300;400;500;600&display=swap');

    :root {
        --bg-color: #0d1117;
        --glass-bg: rgba(22, 27, 34, 0.7);
        --glass-border: rgba(255, 255, 255, 0.1);
        --accent: #58a6ff;
        --accent-glow: rgba(88, 166, 255, 0.4);
        --success: #3fb950;
        --warning: #d29922;
        --error: #f85149;
        --text-primary: #f0f6fc;
        --text-secondary: #8b949e;
    }

    /* Global Transitions & Fonts */
    html, body, [data-testid="stAppViewContainer"] {
        font-family: 'Inter', sans-serif;
        background: radial-gradient(circle at top right, #161b22, #0d1117);
    }

    h1, h2, h3 { font-family: 'Outfit', sans-serif; font-weight: 700; letter-spacing: -0.02em; }

    /* Glass Panels */
    div[data-testid="stMetric"], .crm-card, div[data-testid="stExpander"] {
        background: var(--glass-bg) !important;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid var(--glass-border) !important;
        border-radius: 12px !important;
        padding: 20px !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
    }

    .crm-card:hover {
        transform: translateY(-5px);
        border-color: var(--accent) !important;
        box-shadow: 0 12px 40px 0 rgba(88, 166, 255, 0.15);
    }

    /* Custom Metric Display */
    .metric-value {
        font-size: 2.2rem;
        font-weight: 800;
        color: var(--text-primary);
        font-family: 'Outfit', sans-serif;
        margin: 5px 0;
    }
    .metric-label {
        font-size: 0.85rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }

    /* Sidebar Glassmorphism */
    [data-testid="stSidebar"] {
        background: rgba(1, 4, 9, 0.8) !important;
        backdrop-filter: blur(20px);
        border-right: 1px solid var(--glass-border);
    }

    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre;
        background-color: transparent;
        border-radius: 8px 8px 0 0;
        color: var(--text-secondary);
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent) !important;
        border-bottom: 2px solid var(--accent) !important;
    }

    /* Inputs & Forms */
    input, textarea, [data-baseweb="select"] {
        background-color: rgba(0,0,0,0.2) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 8px !important;
        color: white !important;
    }

    /* Buttons */
    button[kind="primary"] {
        background: linear-gradient(135deg, #238636, #2ea043) !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.6rem 2rem !important;
        font-weight: 700 !important;
        box-shadow: 0 4px 14px 0 rgba(46, 160, 67, 0.39) !important;
    }
    
    button[kind="secondary"] {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid var(--glass-border) !important;
    }

    /* Custom Badges */
    .badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        background: rgba(88, 166, 255, 0.1);
        color: var(--accent);
        border: 1px solid var(--accent);
    }
</style>
"""

st.markdown(CRM_CSS, unsafe_allow_html=True)

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
from database.service import service as db_service


@st.cache_resource
def get_db():
    return db_service


# ── Initialization ───────────────────────────────────────────
if "db_initialized" not in st.session_state:
    st.session_state.db_initialized = False
    st.session_state.browsers_installed = check_browser_install()

# ── Sidebar Setup ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("<h2 style='text-align: center; margin-bottom: 0;'>🛡️ LeadStealth</h2>", unsafe_allow_html=True)
    st.markdown("<div style='text-align: center; color: var(--text-secondary); font-size: 0.9rem; margin-bottom: 30px;'>CRM Edition v3.0</div>", unsafe_allow_html=True)

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
    """Load leads from DB service"""
    return get_db().load_leads()


def save_leads(df):
    """Save leads to DB service (Migration/Batch update)"""
    # In the new service, we usually add/update leads individually or via batch
    # For compatibility with existing UI logic that might pass a whole DF:
    for _, row in df.iterrows():
        get_db().add_or_update_lead(row.to_dict())


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
    st.markdown("### ⚙️ Integrations")
    st.caption("HostFast Free Webmail")

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
    st.header("🔍 Lead Discovery")
    
    wizard_step = st.radio("Scrape Progress", ["1. Criteria", "2. Source Setup", "3. Extraction"], horizontal=True, label_visibility="collapsed")
    st.divider()

    if wizard_step == "1. Criteria":
        st.markdown("### 🎯 Target Audience")
        col1, col2 = st.columns(2)
        with col1:
            query = st.text_input("Business Category", value="Plumbers", help="e.g., Plumbers, HVAC, Dentists", key="wiz_query")
        with col2:
            location = st.text_input("Location", value="Austin, TX", help="City or ZIP", key="wiz_loc")
            
        st.markdown("### ⚙️ Channel Selection")
        sources = st.multiselect("Active Channels", ["Google Maps", "Yellow Pages", "Yelp"], default=["Google Maps"], key="wiz_sources")
        st.info("💡 Pro Tip: Selecting multiple channels increases lead diversity.")
        
    elif wizard_step == "2. Source Setup":
        st.markdown("### 🛠️ Advanced Parameters")
        col1, col2 = st.columns(2)
        with col1:
            max_leads = st.number_input("Lead Limit", value=50, min_value=10, key="wiz_max")
        with col2:
            # Check cloud
            is_cloud = os.environ.get("STREAMLIT_SERVER_PORT") is not None
            headful = st.checkbox("Show Browser (Local Only)", value=not is_cloud, disabled=is_cloud, key="wiz_headful")
            
        if not get_db().use_supabase:
            st.warning("⚠️ **Local Storage**: Data won't persist after restart. [Connect Supabase](https://supabase.com)")
            
    elif wizard_step == "3. Extraction":
        st.markdown("### 🚀 Live Console")
        
        if st.session_state.scanning:
            st.markdown(f"""
            <div style="background: rgba(0,0,0,0.5); border: 1px solid var(--accent); border-radius: 8px; padding: 20px; font-family: monospace; color: var(--accent); margin-bottom: 20px;">
                <span style="color: #666;">[SYSTEM]</span> INITIALIZING ENGINES...<br>
                <span style="color: #666;">[SCRAPER]</span> TARGETING: <span style="color: #fff;">{st.session_state.get('wiz_query', 'Plumbers')}</span> IN <span style="color: #fff;">{st.session_state.get('wiz_loc', 'Austin, TX')}</span>...<br>
                <span style="color: #666;">[STATUS]</span> <span style="color: var(--success);">{st.session_state.status_msg or 'Starting...'}</span>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("🛑 Terminate Scrape", type="secondary"):
                if st.session_state.process:
                    st.session_state.process.terminate()
                st.session_state.scanning = False
                st.session_state.process = None
                st.rerun()
        else:
            if st.button("⚡ Start New Extraction", type="primary", use_container_width=True):
                # Build source list for CLI
                source_map = {"Google Maps": "google_maps", "Yellow Pages": "yellowpages", "Yelp": "yelp"}
                wiz_sources = st.session_state.get("wiz_sources", ["Google Maps"])
                source_args = ",".join(source_map[s] for s in wiz_sources if s in source_map)

                cmd = [
                    sys.executable, 
                    "run_scraper_cli.py", 
                    "--query", st.session_state.get("wiz_query", "Plumbers"), 
                    "--location", st.session_state.get("wiz_loc", "Austin, TX"), 
                    "--sources", source_args
                ]
                if st.session_state.get("wiz_headful", True): cmd.append("--headful")

                st.session_state.process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, 
                    cwd=os.path.dirname(os.path.abspath(__file__))
                )
                st.session_state.scanning = True
                st.rerun()

            if st.session_state.status_msg:
                st.success(st.session_state.status_msg)

    # Background process polling
    if st.session_state.scanning and st.session_state.process:
        retcode = st.session_state.process.poll()
        if retcode is not None:
            st.session_state.scanning = False
            stdout, stderr = st.session_state.process.communicate()
            for line in stdout.strip().splitlines():
                if line.startswith("STATUS:"):
                    st.session_state.status_msg = line.replace("STATUS:", "")
            st.session_state.leads_df = load_leads()
            st.session_state.process = None
            st.rerun()


# ── Tab 2: Email Campaigns ───────────────────────────────────

with tab2:
    st.markdown("### ✉️ Campaign Manager")
    st.markdown("<div style='margin-bottom: 20px;'>Design and dispatch email sequences directly from your integrated webmail profiles.</div>", unsafe_allow_html=True)

    if not st.session_state.webmail_accounts:
        st.error("⚠️ Please add at least one webmail account in the sidebar first!")
    elif st.session_state.leads_df.empty:
        st.warning("No leads found. Scrape some leads first!")
    else:
        # Load templates
        templates = WebmailTemplateLibrary.get_templates()

        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2 = st.columns([1, 1.2], gap="large")

        with col1:
            st.markdown("#### 👥 Filter Audience")
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
            st.markdown("#### 📝 Compose Template")
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
        st.markdown("<hr style='border-color: var(--border-color); margin: 30px 0;'>", unsafe_allow_html=True)
        st.markdown("#### 🚀 Dispatch Settings")
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

                try:
                    if sys.platform == "win32":
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        results = loop.run_until_complete(run_campaign())
                    else:
                        results = asyncio.run(run_campaign())
                    st.session_state.campaign_stats = results
                    st.session_state.sending = False

                    # Process detailed results
                    current_session_id = datetime.now().strftime("%Y%m%d%H%M%S")
                    details = results.get("details", [])
                    
                    for detail in details:
                        # Find the lead ID from matches
                        target_lead = leads_filtered[leads_filtered['email'] == detail['email']]
                        if not target_lead.empty:
                            lead_id = int(target_lead.iloc[0].get('id'))
                            
                            if detail['success']:
                                # Record in DB
                                get_db().record_email_sent(
                                    lead_id=lead_id,
                                    subject=detail['subject'],
                                    body="", # Body not stored for privacy/space unless asked
                                    status="sent"
                                )
                            else:
                                # Log failure
                                get_db().log_activity(
                                    lead_id=lead_id,
                                    activity_type="email_failed",
                                    description=f"Failed to send email: {detail.get('error')}"
                                )

                    # Refresh local state
                    st.session_state.leads_df = load_leads()
                    st.session_state.campaign_stats = results
                    st.session_state.sending = False

                    # Save campaign session to history
                    get_db().save_session(
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
                st.markdown(f'<div class="crm-card"><h3>Total Checked</h3><h2>{stats["total"]}</h2></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="crm-card"><h3>Emails Sent</h3><h2 style="color: var(--success);">{stats["sent"]}</h2></div>', unsafe_allow_html=True)
            with col3:
                fail_color = "#f85149" if stats["failed"] > 0 else "var(--text-primary)"
                st.markdown(f'<div class="crm-card"><h3>Failed</h3><h2 style="color: {fail_color};">{stats["failed"]}</h2></div>', unsafe_allow_html=True)

            if stats.get("errors"):
                with st.expander("❌ View Error Logs"):
                    for error in stats["errors"][:10]:
                        st.error(error)


# ── Tab 3: Analytics ─────────────────────────────────────────

with tab3:
    st.header("📊 Lead Database")

    df = st.session_state.leads_df

    if df.empty:
        st.info("No leads yet. Go to the Lead Scraping tab to find some!")
    else:
        # Stats (Custom CRM Cards)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'''
                <div class="crm-card">
                    <div class="metric-label">Total Leads</div>
                    <div class="metric-value">{len(df)}</div>
                </div>
            ''', unsafe_allow_html=True)
        with col2:
            has_email = df["email"].notna().sum() if "email" in df.columns else 0
            st.markdown(f'''
                <div class="crm-card">
                    <div class="metric-label">With Email</div>
                    <div class="metric-value" style="color: var(--accent);">{has_email}</div>
                </div>
            ''', unsafe_allow_html=True)
        with col3:
            has_website = df["website"].notna().sum() if "website" in df.columns else 0
            st.markdown(f'''
                <div class="crm-card">
                    <div class="metric-label">With Website</div>
                    <div class="metric-value">{has_website}</div>
                </div>
            ''', unsafe_allow_html=True)
        with col4:
            contacted = 0
            if "campaign_status" in df.columns:
                contacted = df[df["campaign_status"].isin(["contacted", "Sent"])].shape[0]
            st.markdown(f'''
                <div class="crm-card">
                    <div class="metric-label">Sent/Contacted</div>
                    <div class="metric-value" style="color: var(--success);">{contacted}</div>
                </div>
            ''', unsafe_allow_html=True)

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

            # Interactive Dataframe for CRM
            try:
                df_event = st.dataframe(
                    display_df,
                    on_select="rerun",
                    selection_mode="single-row",
                    column_config={
                        "website": st.column_config.LinkColumn("Website"),
                        "email": st.column_config.LinkColumn("Email"),
                        "facebook": st.column_config.LinkColumn("Facebook"),
                        "instagram": st.column_config.LinkColumn("Instagram"),
                        "linkedin": st.column_config.LinkColumn("LinkedIn"),
                        "twitter": st.column_config.LinkColumn("Twitter"),
                        "yelp_url": st.column_config.LinkColumn("Yelp"),
                        "campaign_status": st.column_config.TextColumn("Status"),
                        "rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                        "review_count": st.column_config.NumberColumn("Reviews"),
                    },
                    width="stretch",
                )
                
                # Render Lead Details if selected
                selected_rows = []
                if hasattr(df_event, "selection"):
                    selected_rows = df_event.selection.rows
                elif isinstance(df_event, dict):
                    selected_rows = df_event.get("selection", {}).get("rows", [])
                    
                if selected_rows and len(selected_rows) > 0:
                    st.markdown("<hr style='border-color: var(--border-color);'>", unsafe_allow_html=True)
                    lead = display_df.iloc[selected_rows[0]]
                    lead_id = int(lead.get('id'))
                    
                    st.markdown("### 🔍 Lead Details Profile")
                    
                    det1, det2, det3 = st.columns([2, 1, 1])
                    with det1:
                        st.markdown(f"**Name:** {lead.get('name', 'N/A')}")
                        st.markdown(f"**Email:** {lead.get('email', 'N/A')}")
                        st.markdown(f"**Phone:** {lead.get('phone', 'N/A')}")
                        st.markdown(f"**Location:** {lead.get('city', '')}, {lead.get('state', '')}")
                    with det2:
                        st.markdown("**Technologies Detected**")
                        if lead.get('wordpress'): st.markdown("✅ WordPress")
                        if lead.get('shopify'): st.markdown("✅ Shopify")
                        if lead.get('cms'): st.markdown(f"CMS: `{lead.get('cms')}`")
                    with det3:
                        st.markdown("**Pipeline Stage**")
                        status_options = ["new", "contacted", "responded", "bounced", "converted"]
                        current_status = lead.get("campaign_status", "new")
                        if pd.isna(current_status): current_status = "new"
                        
                        try:
                            new_status = st.selectbox("Update Status", status_options, index=status_options.index(current_status))
                            if new_status != current_status:
                                if db_service.update_lead_status(lead_id, new_status):
                                    st.success(f"Status updated to {new_status}!")
                                    st.rerun()
                        except:
                            st.markdown(f"<div class='badge'>{current_status}</div>", unsafe_allow_html=True)
                    
                    # Activity Timeline
                    st.divider()
                    st.markdown("#### 🕐 Activity History")
                    activities = db_service.get_lead_activities(lead_id)
                    
                    if not activities:
                        st.caption("No activity recorded for this lead yet.")
                    else:
                        for act in activities:
                            icon = "📧" if "email" in act['type'] else "📝"
                            with st.container():
                                st.markdown(f"""
                                <div style="border-left: 2px solid var(--accent); padding-left: 15px; margin-bottom: 10px;">
                                    <span style="color: var(--text-secondary); font-size: 0.8rem;">{act['date']}</span><br>
                                    <strong>{icon} {act['type'].replace('_', ' ').title()}</strong>: {act['description']}
                                </div>
                                """, unsafe_allow_html=True)
                        
            except Exception as e:
                # Fallback for older streamlit versions without on_select
                st.dataframe(display_df, width="stretch")

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
            sessions_df = get_db().get_sessions_df()
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
                    width="stretch",
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
