"""
Database models using SQLAlchemy - Production grade replacement for CSV storage
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean, 
    Float, Text, ForeignKey, Index, event
)
from sqlalchemy.orm import DeclarativeBase, relationship, Session
from sqlalchemy.pool import QueuePool
from pydantic import BaseModel, Field
import json
import os

class Base(DeclarativeBase):
    pass

# ── Database Models ───────────────────────────────────────────

class Lead(Base):
    """Core lead entity - replaces CSV storage"""
    __tablename__ = 'leads'
    
    id = Column(Integer, primary_key=True)
    
    # Business Info
    name = Column(String(255), nullable=False, index=True)
    source = Column(String(50), nullable=False, index=True)  # google_maps, yellow_pages, yelp, etc.
    category = Column(String(500), index=True)
    
    # Location Details
    location = Column(String(500), index=True)
    city = Column(String(100), index=True)
    state = Column(String(100), index=True)
    zip = Column(String(20), index=True)
    address = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Business Details
    rating = Column(Float)
    review_count = Column(Integer)
    hours = Column(Text) # JSON
    business_status = Column(String(100))
    verified = Column(Boolean)
    
    # Contact Data
    website = Column(String(500))
    phone = Column(String(50))
    email = Column(String(255), index=True)
    email_verified = Column(Boolean, default=False)
    email_status = Column(String(50))  # valid, invalid, risky, unknown
    
    # Social Profiles
    facebook = Column(String(500))
    instagram = Column(String(500))
    linkedin = Column(String(500))
    twitter = Column(String(500))
    youtube = Column(String(500))
    tiktok = Column(String(500))
    pinterest = Column(String(500))
    
    # Tech Detection
    cms = Column(String(100))
    technologies = Column(Text) # Comma separated
    wordpress = Column(Boolean)
    shopify = Column(Boolean)
    
    # Content & Search
    description = Column(Text)
    services = Column(Text)
    place_id = Column(String(255), index=True)
    cid = Column(String(255))
    google_maps_url = Column(String(500))
    yelp_url = Column(String(500))
    yellow_pages_url = Column(String(500))
    
    # Scraping Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow)
    last_enriched = Column(DateTime)
    enrichment_count = Column(Integer, default=0)
    source_url = Column(String(500))
    session_id = Column(String(100), index=True)
    
    # Quality Scoring
    lead_score = Column(Float, default=0.0)  # 0-100 based on data completeness
    email_confidence = Column(Float, default=0.0)  # Email verification confidence
    
    # Campaign Status
    campaign_status = Column(String(50), default='new')  # new, contacted, responded, converted, bounced
    last_contact_at = Column(DateTime)
    contact_count = Column(Integer, default=0)
    
    # Deduplication
    domain = Column(String(255), index=True)  # Extracted from website
    hash_key = Column(String(64), unique=True, index=True)  # name+location hash
    
    # Raw Data Storage
    raw_data = Column(Text)  # JSON backup of original scrape
    
    # Relationships
    emails_sent = relationship("EmailSent", back_populates="lead", cascade="all, delete-orphan")
    activities = relationship("LeadActivity", back_populates="lead", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_lead_score', 'lead_score', 'campaign_status'),
        Index('idx_source_category', 'source', 'category'),
    )


class EmailSent(Base):
    """Track all emails sent to leads"""
    __tablename__ = 'emails_sent'
    
    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, ForeignKey('leads.id'), nullable=False)
    
    campaign_id = Column(String(100), index=True)
    template_id = Column(String(100))
    subject = Column(String(500))
    sent_at = Column(DateTime, default=datetime.utcnow)
    
    # Delivery tracking
    delivered = Column(Boolean, default=False)
    opened = Column(Boolean, default=False)
    clicked = Column(Boolean, default=False)
    replied = Column(Boolean, default=False)
    bounced = Column(Boolean, default=False)
    
    # Timestamps
    opened_at = Column(DateTime)
    clicked_at = Column(DateTime)
    replied_at = Column(DateTime)
    
    # Reply content (if captured)
    reply_content = Column(Text)
    
    lead = relationship("Lead", back_populates="emails_sent")


class LeadActivity(Base):
    """Activity log for each lead"""
    __tablename__ = 'lead_activities'
    
    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, ForeignKey('leads.id'), nullable=False)
    
    activity_type = Column(String(50), nullable=False)  # scraped, enriched, emailed, visited_website
    description = Column(Text)
    activity_metadata = Column(Text)  # JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    
    lead = relationship("Lead", back_populates="activities")


class Campaign(Base):
    """Email campaigns"""
    __tablename__ = 'campaigns'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Targeting
    source_filter = Column(String(50))  # Filter by source
    category_filter = Column(String(100))  # Filter by category
    location_filter = Column(String(255))
    min_lead_score = Column(Float, default=0)
    
    # Template
    template_subject = Column(String(500))
    template_body = Column(Text)
    
    # Scheduling
    status = Column(String(50), default='draft')  # draft, active, paused, completed
    scheduled_start = Column(DateTime)
    scheduled_end = Column(DateTime)
    
    # Rate limiting
    emails_per_day = Column(Integer, default=50)
    send_interval_min = Column(Integer, default=60)  # seconds between emails
    
    # Stats
    total_leads = Column(Integer, default=0)
    emails_sent = Column(Integer, default=0)
    emails_delivered = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    emails_clicked = Column(Integer, default=0)
    replies_received = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProxyPool(Base):
    """Manage proxy rotation"""
    __tablename__ = 'proxy_pool'
    
    id = Column(Integer, primary_key=True)
    ip = Column(String(100), nullable=False)
    port = Column(Integer, nullable=False)
    protocol = Column(String(10), default='http')  # http, https, socks5
    
    # Status
    is_active = Column(Boolean, default=True)
    fail_count = Column(Integer, default=0)
    last_used = Column(DateTime)
    last_success = Column(DateTime)
    
    # Performance
    avg_response_time = Column(Float)
    success_rate = Column(Float, default=0.0)
    
    # Rotation tracking
    rotation_weight = Column(Integer, default=1)
    country = Column(String(10))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (Index('idx_active_proxies', 'is_active', 'fail_count'),)


# ── Pydantic Schemas (for API/validation) ────────────────────

class LeadCreate(BaseModel):
    name: str
    source: str
    category: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    facebook: Optional[str] = None
    instagram: Optional[str] = None
    linkedin: Optional[str] = None


class LeadUpdate(BaseModel):
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    campaign_status: Optional[str] = None
    lead_score: Optional[float] = None


class CampaignCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source_filter: Optional[str] = None
    category_filter: Optional[str] = None
    template_subject: str
    template_body: str
    emails_per_day: int = Field(default=50, ge=1, le=500)


# Database Manager and singleton removed. Use database/service.py instead.


# ── Event Listeners ──────────────────────────────────────────

@event.listens_for(Lead, 'before_insert')
def generate_hash_key(mapper, connection, target):
    """Auto-generate deduplication hash"""
    import hashlib
    key = f"{target.name.lower().strip()}:{target.location.lower().strip() if target.location else ''}"
    target.hash_key = hashlib.sha256(key.encode()).hexdigest()
    
    # Extract domain from website
    if target.website:
        from urllib.parse import urlparse
        try:
            parsed = urlparse(target.website)
            target.domain = parsed.netloc.lower().replace('www.', '')
        except:
            pass


@event.listens_for(Lead, 'before_update')
def update_timestamps(mapper, connection, target):
    """Auto-update enrichment timestamp"""
    target.last_enriched = datetime.utcnow()
    target.enrichment_count += 1
