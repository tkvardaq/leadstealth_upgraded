import os
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
from sqlalchemy import create_engine, select, update, delete, and_, or_
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool
from .models import Base, Lead, EmailSent, LeadActivity, Campaign

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LeadStealth.DB")

class DatabaseService:
    """
    Unified database service for LeadStealth.
    Handles local SQLite (via SQLAlchemy) and optional Supabase sync.
    """
    def __init__(self, db_path: str = "leadstealth.db"):
        self.db_path = db_path
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Supabase config (optional)
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.use_supabase = False
        self.supabase_client = None
        
        if self.supabase_url and self.supabase_key:
            try:
                from supabase import create_client
                self.supabase_client = create_client(self.supabase_url, self.supabase_key)
                self.use_supabase = True
                logger.info("Supabase connected for cloud sync.")
            except ImportError:
                logger.warning("Supabase package not installed. Skipping cloud sync.")
            except Exception as e:
                logger.error(f"Supabase connection failed: {e}")

    def get_session(self) -> Session:
        return self.SessionLocal()

    # ── Leads Management ───────────────────────────────────────────

    def load_leads(self, filters: Optional[Dict] = None, limit: int = 1000) -> pd.DataFrame:
        """Load leads into a pandas DataFrame for UI display"""
        with self.get_session() as session:
            query = session.query(Lead)
            
            if filters:
                if filters.get("source"):
                    query = query.filter(Lead.source == filters["source"])
                if filters.get("category"):
                    query = query.filter(Lead.category.ilike(f"%{filters['category']}%"))
                if filters.get("campaign_status"):
                    query = query.filter(Lead.campaign_status == filters["campaign_status"])
                if filters.get("has_email"):
                    query = query.filter(Lead.email.isnot(None))
            
            leads = query.limit(limit).all()
            
            # Convert to list of dicts for DataFrame
            data = []
            for lead in leads:
                d = {c.name: getattr(lead, c.name) for c in lead.__table__.columns}
                data.append(d)
                
            df = pd.DataFrame(data) if data else pd.DataFrame()
            
            if not df.empty:
                # Ensure Arrow-compatible types for Streamlit
                if 'review_count' in df.columns:
                    df['review_count'] = pd.to_numeric(df['review_count'], errors='coerce').astype('Int64')
                if 'rating' in df.columns:
                    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype('float64')
                
                # Ensure string columns don't have None for Arrow
                str_cols = ['name', 'email', 'phone', 'website', 'city', 'state', 'campaign_status']
                for col in str_cols:
                    if col in df.columns:
                        df[col] = df[col].fillna("")
            
            return df

    def add_or_update_lead(self, lead_data: Dict) -> Lead:
        """Add a new lead or update an existing one based on hash_key"""
        with self.get_session() as session:
            # Generate hash_key if not provided (logic from models.py event listener will handle it too)
            name = lead_data.get("name", "").lower().strip()
            location = lead_data.get("location", "").lower().strip()
            import hashlib
            hash_key = hashlib.sha256(f"{name}:{location}".encode()).hexdigest()
            
            # Check for existing
            stmt = select(Lead).where(Lead.hash_key == hash_key)
            existing_lead = session.execute(stmt).scalar_one_or_none()
            
            if existing_lead:
                # Update
                for key, value in lead_data.items():
                    if hasattr(existing_lead, key) and value is not None:
                        setattr(existing_lead, key, value)
                existing_lead.updated_at = datetime.utcnow()
                lead = existing_lead
            else:
                # Create
                lead = Lead(**lead_data)
                session.add(lead)
            
            session.commit()
            session.refresh(lead)
            
            # Sync to Supabase if enabled
            if self.use_supabase:
                self._sync_lead_to_supabase(lead)
                
            return lead

    def _sync_lead_to_supabase(self, lead: Lead):
        """Push a lead to Supabase"""
        try:
            data = {c.name: getattr(lead, c.name) for c in lead.__table__.columns}
            # Handle datetime serialization
            for k, v in data.items():
                if isinstance(v, datetime):
                    data[k] = v.isoformat()
            
            self.supabase_client.table("leads").upsert(data).execute()
        except Exception as e:
            logger.error(f"Supabase sync failed for lead {lead.id}: {e}")

    # ── Session / Activity Management ──────────────────────────────

    def log_activity(self, lead_id: int, activity_type: str, description: str, activity_metadata: Optional[Dict] = None):
        """Log an activity and update lead's last contact if needed"""
        with self.get_session() as session:
            activity = LeadActivity(
                lead_id=lead_id,
                activity_type=activity_type,
                description=description,
                activity_metadata=json.dumps(activity_metadata) if activity_metadata else None
            )
            session.add(activity)
            
            # Auto-update lead status/count for common activities
            if activity_type in ["email_sent", "contacted"]:
                lead = session.query(Lead).get(lead_id)
                if lead:
                    lead.campaign_status = "contacted"
                    lead.last_contact_at = datetime.utcnow()
                    lead.contact_count += 1
            
            session.commit()

    def record_email_sent(self, lead_id: int, subject: str, body: str, status: str = "sent"):
        """Record an email sent to a lead"""
        with self.get_session() as session:
            record = EmailSent(
                lead_id=lead_id,
                subject=subject,
                sent_at=datetime.utcnow(),
                delivered=(status == "sent")
            )
            session.add(record)
            
            # Also log as activity
            self.log_activity(
                lead_id=lead_id, 
                activity_type="email_sent", 
                description=f"Sent email: {subject}"
            )
            
            session.commit()

    def get_sessions_df(self) -> pd.DataFrame:
        """Load session history for UI"""
        # For now, we reuse the JSON session file if it exists, otherwise use SQLite
        SESSIONS_FILE = "sessions.db.json"
        if os.path.exists(SESSIONS_FILE):
            try:
                with open(SESSIONS_FILE, "r", encoding='utf-8') as f:
                    data = json.load(f)
                    return pd.DataFrame(data)
            except:
                pass
        return pd.DataFrame()

    def save_session(self, session_data: Dict):
        """Save a new session to the JSON file (legacy support)"""
        SESSIONS_FILE = "sessions.db.json"
        sessions = []
        if os.path.exists(SESSIONS_FILE):
            try:
                with open(SESSIONS_FILE, "r", encoding='utf-8') as f:
                    sessions = json.load(f)
            except:
                pass
        
        sessions.append(session_data)
        with open(SESSIONS_FILE, "w", encoding='utf-8') as f:
            json.dump(sessions, f, indent=2, default=str)

    def get_lead_activities(self, lead_id: int) -> List[Dict]:
        """Get history for a specific lead"""
        with self.get_session() as session:
            activities = session.query(LeadActivity).filter(LeadActivity.lead_id == lead_id).order_by(LeadActivity.created_at.desc()).all()
            return [
                {
                    "type": a.activity_type,
                    "description": a.description,
                    "date": a.created_at.strftime("%Y-%m-%d %H:%M"),
                    "metadata": json.loads(a.activity_metadata) if a.activity_metadata else None
                } for a in activities
            ]
            
    def update_lead_status(self, lead_id: int, status: str):
        """Manually update lead status"""
        with self.get_session() as session:
            lead = session.query(Lead).get(lead_id)
            if lead:
                lead.campaign_status = status
                # If we have a Session object, we should use it consistently
                # But here we are within a with self.get_session() as session block
                # so session is available.
                self.log_activity(lead_id, "status_change", f"Status updated to: {status}")
                session.commit()
                return True
            return False

    # ── Utility: Migration ────────────────────────────────────────

    def migrate_from_csv(self, csv_path: str = "leads.db.csv"):
        """Import data from legacy CSV into SQLite"""
        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found: {csv_path}")
            return
            
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Migrating {len(df)} leads from {csv_path}...")
            
            count = 0
            # Get valid Lead column names
            valid_columns = {c.name for c in Lead.__table__.columns}
            
            for _, row in df.iterrows():
                # Map CSV columns to Lead model
                row_dict = row.dropna().to_dict()
                lead_dict = {k: v for k, v in row_dict.items() if k in valid_columns}
                
                # Special mapping for socials if needed (e.g. facebook_url -> facebook)
                # But our CSV already has facebook, instagram, etc.
                
                # Cleanup common CSV issues
                if 'id' in lead_dict: del lead_dict['id']
                
                if not lead_dict.get('name'):
                    continue
                    
                self.add_or_update_lead(lead_dict)
                count += 1
                if count % 50 == 0:
                    logger.info(f"Migrated {count} leads...")
            
            logger.info("Migration complete.")
        except Exception as e:
            logger.error(f"Migration failed: {e}")

# Singleton instance
service = DatabaseService()
