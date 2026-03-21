"""
Email Infrastructure - Campaigns, Templates, SMTP Rotation, Tracking
"""
import asyncio
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
from email.header import Header
from email_validator import validate_email, EmailNotValidError
from jinja2 import Template, Environment, BaseLoader
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import random
import json
import re
import os
from pathlib import Path


@dataclass
class SMTPConfig:
    """SMTP account configuration"""
    host: str
    port: int
    username: str
    password: str
    sender_name: str
    sender_email: str
    use_tls: bool = True
    daily_limit: int = 50
    hourly_limit: int = 15
    warmup_mode: bool = True


class SMTPRotator:
    """
    Rotates through multiple SMTP accounts to avoid rate limits
    and build sender reputation gradually (warmup mode)
    """
    
    def __init__(self):
        self.accounts: List[SMTPConfig] = []
        self.account_usage: Dict[int, Dict] = {}  # Track usage per account
        self.current_index = 0
        
    def add_account(self, config: SMTPConfig):
        """Add SMTP account to rotation pool"""
        idx = len(self.accounts)
        self.accounts.append(config)
        self.account_usage[idx] = {
            'daily_sent': 0,
            'hourly_sent': 0,
            'last_reset_day': datetime.now(),
            'last_reset_hour': datetime.now(),
            'total_sent': 0,
            'warmup_day': 1,
            'warmup_count': self._calculate_warmup_limit(1)
        }
    
    def _calculate_warmup_limit(self, day: int) -> int:
        """Calculate daily send limit based on warmup day"""
        # Industry-standard warmup schedule
        warmup_schedule = {
            1: 5, 2: 10, 3: 15, 4: 25, 5: 40,
            6: 60, 7: 80, 8: 100, 9: 150, 10: 200,
            11: 300, 12: 400, 13: 500, 14: 750
        }
        return warmup_schedule.get(day, 1000)  # Full volume after 2 weeks
    
    def get_next_account(self) -> Optional[Tuple[int, SMTPConfig]]:
        """Get next available SMTP account respecting rate limits"""
        now = datetime.now()
        
        for _ in range(len(self.accounts)):
            idx = self.current_index % len(self.accounts)
            self.current_index += 1
            
            usage = self.account_usage[idx]
            account = self.accounts[idx]
            
            # Reset counters if needed
            if now - usage['last_reset_day'] > timedelta(days=1):
                usage['daily_sent'] = 0
                usage['last_reset_day'] = now
                if account.warmup_mode:
                    usage['warmup_day'] = min(usage['warmup_day'] + 1, 14)
                    usage['warmup_count'] = self._calculate_warmup_limit(usage['warmup_day'])
            
            if now - usage['last_reset_hour'] > timedelta(hours=1):
                usage['hourly_sent'] = 0
                usage['last_reset_hour'] = now
            
            # Check limits
            effective_daily = usage['warmup_count'] if account.warmup_mode else account.daily_limit
            
            if (usage['daily_sent'] < effective_daily and 
                usage['hourly_sent'] < account.hourly_limit):
                return idx, account
        
        return None
    
    def increment_usage(self, account_idx: int):
        """Track that an email was sent"""
        self.account_usage[account_idx]['daily_sent'] += 1
        self.account_usage[account_idx]['hourly_sent'] += 1
        self.account_usage[account_idx]['total_sent'] += 1
    
    def get_stats(self) -> Dict:
        """Get usage statistics for all accounts"""
        return {
            'accounts': len(self.accounts),
            'total_sent': sum(u['total_sent'] for u in self.account_usage.values()),
            'details': [
                {
                    'index': i,
                    'sender': acc.sender_email,
                    'daily_sent': self.account_usage[i]['daily_sent'],
                    'hourly_sent': self.account_usage[i]['hourly_sent'],
                    'warmup_day': self.account_usage[i]['warmup_day'],
                    'warmup_limit': self.account_usage[i]['warmup_count']
                }
                for i, acc in enumerate(self.accounts)
            ]
        }


class EmailTemplate:
    """Jinja2 email template with personalization"""
    
    def __init__(self, subject: str, body_html: str, body_text: Optional[str] = None):
        self.subject_template = Template(subject)
        self.html_template = Template(body_html)
        self.text_template = Template(body_text) if body_text else None
    
    def render(self, lead_data: Dict) -> Tuple[str, str, str]:
        """Return (subject, html_body, text_body)"""
        # Add default fallbacks
        context = {
            'company': lead_data.get('name', 'there'),
            'website': lead_data.get('website', ''),
            'location': lead_data.get('location', ''),
            'first_name': self._extract_first_name(lead_data.get('name', '')),
            **lead_data
        }
        
        subject = self.subject_template.render(**context)
        html = self.html_template.render(**context)
        
        # Auto-generate text version if not provided
        if self.text_template:
            text = self.text_template.render(**context)
        else:
            text = self._html_to_text(html)
        
        return subject, html, text
    
    @staticmethod
    def _extract_first_name(full_name: str) -> str:
        """Extract first name from business or person name"""
        # Try to extract first name
        cleaned = re.sub(r'[^\w\s&.,]', '', full_name)
        words = cleaned.split()
        if words:
            # Skip articles
            skip_words = ['the', 'a', 'an', 'at', 'in']
            for word in words:
                if word.lower() not in skip_words and len(word) > 1:
                    return word.title()
        return "there"
    
    @staticmethod
    def _html_to_text(html: str) -> str:
        """Convert HTML to plain text"""
        # Simple HTML to text conversion
        text = re.sub(r'<br\s*/?>', '\n', html)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()


class EmailCampaignManager:
    """
    Manages email campaigns with:
    - Template personalization
    - SMTP rotation
    - Rate limiting
    - Tracking/Analytics
    - A/B testing support
    """
    
    def __init__(self, db_manager, smtp_rotator: SMTPRotator):
        self.db = db_manager
        self.smtp = smtp_rotator
        self.templates: Dict[str, EmailTemplate] = {}
        self.tracking_domain = os.getenv('TRACKING_DOMAIN', 'track.yourdomain.com')
        
        # Email tracking pixel and link tracking
        self.tracking_enabled = True
    
    def load_template(self, template_id: str, subject: str, html: str, text: Optional[str] = None):
        """Load a template for use in campaigns"""
        self.templates[template_id] = EmailTemplate(subject, html, text)
    
    def load_templates_from_folder(self, folder: str):
        """Load all templates from a folder"""
        folder_path = Path(folder)
        for file in folder_path.glob('*.json'):
            with open(file) as f:
                data = json.load(f)
                self.load_template(
                    data['id'],
                    data['subject'],
                    data['html'],
                    data.get('text')
                )
    
    def add_tracking(self, html: str, email_id: str) -> str:
        """Add open pixel and click tracking to HTML"""
        if not self.tracking_enabled:
            return html
        
        # Add tracking pixel for opens
        pixel_url = f"https://{self.tracking_domain}/pixel/{email_id}"
        pixel_img = f'<img src="{pixel_url}" width="1" height="1" alt="" />'
        
        # Add tracking to links
        link_pattern = r'href=["\'](https?://[^"\']+)["\']'
        
        def replace_link(match):
            url = match.group(1)
            tracked_url = f"https://{self.tracking_domain}/click/{email_id}?url={url}"
            return f'href="{tracked_url}"'
        
        html = re.sub(link_pattern, replace_link, html)
        
        # Insert pixel before closing body tag
        if '</body>' in html:
            html = html.replace('</body>', f'{pixel_img}\n</body>')
        else:
            html += f'\n{pixel_img}'
        
        return html
    
    async def send_email(
        self,
        to_email: str,
        template_id: str,
        lead_data: Dict,
        email_id: Optional[str] = None
    ) -> Dict:
        """Send a single personalized email"""
        
        # Validate email
        try:
            validation = validate_email(to_email)
            to_email = validation.email
        except EmailNotValidError as e:
            return {'success': False, 'error': str(e)}
        
        # Get SMTP account
        account_info = self.smtp.get_next_account()
        if not account_info:
            return {'success': False, 'error': 'No SMTP accounts available (rate limited)'}
        
        account_idx, config = account_info
        
        # Get template
        template = self.templates.get(template_id)
        if not template:
            return {'success': False, 'error': f'Template {template_id} not found'}
        
        # Render template
        subject, html_body, text_body = template.render(lead_data)
        
        # Add tracking
        if email_id:
            html_body = self.add_tracking(html_body, email_id)
        
        # Build MIME message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = formataddr((config.sender_name, config.sender_email))
        msg['To'] = to_email
        msg['Date'] = datetime.now().strftime('%a, %d %b %Y %H:%M:%S %z')
        
        # Add List-Unsubscribe header (best practice)
        msg['List-Unsubscribe'] = f'<mailto:unsubscribe@{self.tracking_domain}?subject=unsubscribe>'
        msg['Precedence'] = 'bulk'
        msg['X-Campaign-ID'] = lead_data.get('campaign_id', 'default')
        
        # Attach parts
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        
        # Send
        try:
            await aiosmtplib.send(
                msg,
                hostname=config.host,
                port=config.port,
                username=config.username,
                password=config.password,
                start_tls=config.use_tls,
                timeout=30
            )
            
            self.smtp.increment_usage(account_idx)
            
            return {
                'success': True,
                'email_id': email_id,
                'from': config.sender_email,
                'subject': subject,
                'sent_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def send_campaign(
        self,
        lead_ids: List[int],
        template_id: str,
        campaign_id: str,
        delay_range: Tuple[int, int] = (30, 90)  # Random delay between emails
    ) -> Dict:
        """Send campaign to a list of leads with rate limiting"""
        results = {
            'campaign_id': campaign_id,
            'total': len(lead_ids),
            'sent': 0,
            'failed': 0,
            'errors': []
        }
        
        for i, lead_id in enumerate(lead_ids):
            # Get lead data
            with self.db.get_session() as session:
                lead = session.get(Lead, lead_id)
                if not lead or not lead.email:
                    results['failed'] += 1
                    results['errors'].append(f"Lead {lead_id}: No email")
                    continue
                
                lead_data = {
                    'name': lead.name,
                    'company': lead.name,
                    'website': lead.website,
                    'location': lead.location,
                    'email': lead.email,
                    'campaign_id': campaign_id
                }
            
            # Send email
            result = await self.send_email(
                lead.email,
                template_id,
                lead_data,
                email_id=f"{campaign_id}_{lead_id}_{i}"
            )
            
            if result['success']:
                results['sent'] += 1
                
                # Update lead status
                with self.db.get_session() as session:
                    lead = session.get(Lead, lead_id)
                    lead.campaign_status = 'contacted'
                    lead.last_contact_at = datetime.now()
                    lead.contact_count += 1
                    session.commit()
            else:
                results['failed'] += 1
                results['errors'].append(f"Lead {lead_id}: {result.get('error')}")
            
            # Rate limiting delay
            if i < len(lead_ids) - 1:
                delay = random.randint(*delay_range)
                await asyncio.sleep(delay)
        
        return results


# ── Pre-built Template Examples ─────────────────────────────

INTRO_TEMPLATES = {
    'intro_v1': {
        'id': 'intro_v1',
        'subject': '{{ company }} - Quick question about your marketing',
        'html': '''
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <p>Hi {{ first_name }},</p>
            
            <p>I noticed {{ company }}'s website and wanted to reach out. 
            I'm with a digital marketing agency that helps local businesses like yours 
            get more leads.</p>
            
            <p>Quick question: Are you currently running any paid advertising?</p>
            
            <p>If not, I have some ideas that could help {{ company }} get more 
            customers in {{ location }}.</p>
            
            <p>Would you be open to a quick 10-minute chat this week?</p>
            
            <p>Best,<br>
            [Your Name]</p>
            
            <p style="font-size: 12px; color: #666;">
            P.S. If you're not the right person for this, could you point me in the right direction?
            </p>
        </body>
        </html>
        ''',
        'text': '''
Hi {{ first_name }},

I noticed {{ company }}'s website and wanted to reach out. I'm with a digital 
marketing agency that helps local businesses like yours get more leads.

Quick question: Are you currently running any paid advertising?

If not, I have some ideas that could help {{ company }} get more customers 
in {{ location }}.

Would you be open to a quick 10-minute chat this week?

Best,
[Your Name]

P.S. If you're not the right person for this, could you point me in the right direction?
        '''
    },
    
    'follow_up_v1': {
        'id': 'follow_up_v1',
        'subject': 'Re: {{ company }} - Quick question',
        'html': '''
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <p>Hi {{ first_name }},</p>
            
            <p>Following up on my previous email about helping {{ company }} 
            with your digital marketing.</p>
            
            <p>I understand you're busy. If you're interested in learning 
            how we've helped similar businesses in {{ location }} increase their 
            leads, just reply with "YES" and I'll send you a case study.</p>
            
            <p>If this isn't a priority right now, no worries - just let me know 
            and I won't follow up again.</p>
            
            <p>Best,<br>
            [Your Name]</p>
        </body>
        </html>
        '''
    },
    
    'value_proposition_v1': {
        'id': 'value_proposition_v1',
        'subject': 'How {{ company }} can get 50+ more leads/month',
        'html': '''
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <p>Hi {{ first_name }},</p>
            
            <p>Your competitor down the street is getting 50+ leads per month 
            through Google Ads. {{ company }} could be doing the same.</p>
            
            <p>Here's what we do for businesses like yours:</p>
            <ul>
                <li>✓ Targeted Google Ads campaigns</li>
                <li>✓ Landing pages that convert visitors to leads</li>
                <li>✓ Email automation to nurture prospects</li>
            </ul>
            
            <p>I'd love to show you exactly how this would work for {{ company }}.</p>
            
            <p>Are you open to a brief call this week?</p>
            
            <p>Best,<br>
            [Your Name]</p>
            
            <p style="font-size: 12px; color: #666;">
            <a href="https://calendly.com/YOUR_LINK" style="color: #0066cc;">
            Or book a time that works for you here →
            </a>
            </p>
        </body>
        </html>
        '''
    }
}


class TemplateLibrary:
    """Ready-to-use email templates for agencies"""
    
    @classmethod
    def get_template(cls, name: str) -> Dict:
        return INTRO_TEMPLATES.get(name)
    
    @classmethod
    def list_templates(cls) -> List[str]:
        return list(INTRO_TEMPLATES.keys())
    
    @classmethod
    def create_custom(
        cls,
        template_id: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> EmailTemplate:
        """Create a custom template"""
        return EmailTemplate(subject, html_body, text_body)
