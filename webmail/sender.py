"""
Webmail Automation Sender - Uses browser automation to send via cPanel/Roundcube
Works with any hosting provider that uses standard webmail interfaces
"""
import asyncio
import time
import random
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from email_validator import validate_email, EmailNotValidError
from jinja2 import Template
import re
import json
from pathlib import Path
import sys
import os

from utils import install_playwright


@dataclass
class WebmailAccount:
    """Webmail account configuration"""
    name: str  # Your name (e.g., "Hammit")
    email: str  # Full email address
    password: str
    webmail_url: str  # e.g., "https://yourdomain.com:2096" or "https://webmail.hostfast.pk"
    provider: str = "cpanel"  # cpanel, roundcube, squirrelmail, horde
    daily_limit: int = 100  # Most shared hosts allow 100-250/day
    hourly_limit: int = 20
    
    # Status tracking
    sent_today: int = 0
    sent_this_hour: int = 0
    last_reset_day: datetime = None
    last_reset_hour: datetime = None
    
    def __post_init__(self):
        if self.last_reset_day is None:
            self.last_reset_day = datetime.now()
        if self.last_reset_hour is None:
            self.last_reset_hour = datetime.now()


class WebmailSender:
    """
    Send emails through webmail interface using browser automation
    Supports: cPanel, RoundCube, Horde, SquirrelMail
    """
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.accounts: List[WebmailAccount] = []
        self.current_account_idx = 0
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.is_logged_in = False
        self.current_account = None
        
        # Selectors for different webmail providers
        self.selectors = {
            'cpanel': {
                'login_email': 'input[name="user"], input[id="user"], input[placeholder*="email" i]',
                'login_pass': 'input[name="pass"], input[id="pass"], input[type="password"]',
                'login_btn': 'button[type="submit"], input[type="submit"], button:has-text("Log in")',
                'roundcube_link': 'a[href*="roundcube"], .menu-icon.roundcube, [title="RoundCube"]',
                'compose_btn': 'a[href*="compose"], button.compose, #rcmbtn_compose, a:has-text("Compose")',
                'to_field': 'input[name="_to"], #_to, input[placeholder*="To" i]',
                'subject_field': 'input[name="_subject"], #_subject, input[placeholder*="Subject" i]',
                'body_frame': 'textarea[name="_message"], #_message, .mceEditor iframe, #composebody',
                'send_btn': 'button[name="_send"], input[name="_send"], button:has-text("Send")',
                'success_indicator': '.confirmation, .success, .alert-success, #message:has-text("sent")',
                'logout_btn': 'a[href*="logout"], button.logout, a:has-text("Logout")'
            },
            'roundcube': {
                'login_user': 'input[name="_user"], #rcmloginuser',
                'login_pass': 'input[name="_pass"], #rcmloginpwd',
                'login_btn': '#rcmloginsubmit, button[type="submit"]',
                'compose_btn': '#rcmbtn_compose, a.compose',
                'to_field': '#compose_to, input[name="_to"]',
                'subject_field': '#compose_subject, input[name="_subject"]',
                'body_field': '#composebody, textarea[name="_message"]',
                'send_btn': '#rcmbtn_send, button.send, input[type="submit"][value="Send"]',
                'success_msg': '.confirmation, .ui.alert-success'
            }
        }
    
    def add_account(self, account: WebmailAccount):
        """Add a webmail account to rotation"""
        self.accounts.append(account)
        print(f"[Webmail] Added account: {account.email}")
    
    def _check_and_reset_limits(self, account: WebmailAccount):
        """Reset daily/hourly counters if needed"""
        now = datetime.now()
        
        if now - account.last_reset_day > timedelta(days=1):
            account.sent_today = 0
            account.last_reset_day = now
            print(f"[Webmail] Daily limit reset for {account.email}")
        
        if now - account.last_reset_hour > timedelta(hours=1):
            account.sent_this_hour = 0
            account.last_reset_hour = now
    
    def get_available_account(self) -> Optional[WebmailAccount]:
        """Get next account that hasn't hit rate limits"""
        for _ in range(len(self.accounts)):
            idx = self.current_account_idx % len(self.accounts)
            self.current_account_idx += 1
            
            account = self.accounts[idx]
            self._check_and_reset_limits(account)
            
            if (account.sent_today < account.daily_limit and 
                account.sent_this_hour < account.hourly_limit):
                return account
        
        return None
    
    async def start(self):
        """Start browser"""
        install_playwright()
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        )
        self.page = await self.context.new_page()
        
        # Add stealth script
        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
    
    async def close(self):
        """Close browser"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def login(self, account: WebmailAccount) -> bool:
        """Login to webmail"""
        self.current_account = account
        
        try:
            print(f"[Webmail] Logging in as {account.email}...")
            
            # Navigate to webmail
            try:
                await self.page.goto(account.webmail_url, wait_until='networkidle', timeout=30000)
            except Exception as e:
                print(f"[Webmail] Initial goto failed: {e}. Retrying with fresh browser...")
                await self.close()
                await self.start()
                await self.page.goto(account.webmail_url, wait_until='networkidle', timeout=30000)

            await asyncio.sleep(random.uniform(2, 4))
            
            # Detect provider or use specified
            provider = account.provider.lower()
            
            # cPanel login
            if provider == 'cpanel' or 'cpanel' in account.webmail_url:
                return await self._login_cpanel(account)
            elif provider == 'roundcube':
                return await self._login_roundcube(account)
            else:
                # Try generic approach
                return await self._login_generic(account)
                
        except Exception as e:
            print(f"[Webmail] Login error: {e}")
            return False
    
    async def _login_cpanel(self, account: WebmailAccount) -> bool:
        """Login to cPanel webmail"""
        try:
            # Check if we are already seeing the Roundcube/Webmail compose UI
            comp_visible = False
            for sel in ['#rcmbtn_compose', 'a.compose', '#composebody', '.button-compose']:
                try:
                    if await self.page.is_visible(sel, timeout=1000):
                        comp_visible = True
                        break
                except:
                    pass
            if comp_visible:
                self.is_logged_in = True
                print(f"[Webmail] Session active for {account.email}")
                return True

            # If login inputs are present, fill them
            user_field = 'input[name="user"]'
            pass_field = 'input[name="pass"]'
            try:
                if await self.page.is_visible(user_field, timeout=3000):
                    await self.page.fill(user_field, account.email)
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    
                    await self.page.fill(pass_field, account.password)
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    
                    # Press Enter instead of clicking button 
                    await self.page.press(pass_field, 'Enter')
                    try:
                        await self.page.wait_for_load_state('networkidle', timeout=10000)
                    except:
                        pass
                    await asyncio.sleep(random.uniform(3, 5))
            except:
                pass # Already logged in or hidden
            
            # Check if we need to select webmail client (cPanel shows options)
            try:
                if await self.page.is_visible('a[href*="roundcube"]', timeout=3000) or \
                   await self.page.is_visible('text=RoundCube', timeout=1000):
                    await self.page.click('a[href*="roundcube"], text=RoundCube', timeout=3000)
                    await asyncio.sleep(random.uniform(2, 4))
            except:
                pass
            
            # Verify login success
            comp_visible = False
            for sel in ['#rcmbtn_compose', 'a.compose', '#composebody', '.button-compose']:
                try:
                    if await self.page.is_visible(sel, timeout=3000):
                        comp_visible = True
                        break
                except:
                    pass

            if comp_visible:
                self.is_logged_in = True
                print(f"[Webmail] Successfully logged in as {account.email}")
                return True
            else:
                print(f"[Webmail] Login may have failed - checking...")
                # Check for error message
                error_text = await self.page.content()
                if 'invalid' in error_text.lower() or 'failed' in error_text.lower():
                    print(f"[Webmail] Login failed - invalid credentials")
                    return False
                return True
                
        except Exception as e:
            print(f"[Webmail] cPanel login error: {e}")
            return False
    
    async def _login_roundcube(self, account: WebmailAccount) -> bool:
        """Login to RoundCube directly"""
        try:
            await self.page.fill('input[name="_user"]', account.email)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            await self.page.fill('input[name="_pass"]', account.password)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            await self.page.click('#rcmloginsubmit')
            await self.page.wait_for_load_state('networkidle')
            await asyncio.sleep(random.uniform(3, 5))
            
            if await self.page.is_visible('#rcmbtn_compose'):
                self.is_logged_in = True
                return True
            return False
            
        except Exception as e:
            print(f"[Webmail] RoundCube login error: {e}")
            return False
    
    async def _login_generic(self, account: WebmailAccount) -> bool:
        """Try generic login approach"""
        try:
            # Try common selectors
            user_selectors = [
                'input[name="user"]', 'input[name="email"]', 
                'input[name="login"]', 'input[name="username"]',
                'input[type="email"]'
            ]
            pass_selectors = [
                'input[name="pass"]', 'input[name="password"]',
                'input[type="password"]'
            ]
            
            # Find and fill username
            for sel in user_selectors:
                if await self.page.is_visible(sel):
                    await self.page.fill(sel, account.email)
                    break
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Find and fill password
            for sel in pass_selectors:
                if await self.page.is_visible(sel):
                    await self.page.fill(sel, account.password)
                    break
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Click any submit button
            await self.page.click('button[type="submit"], input[type="submit"]')
            await asyncio.sleep(random.uniform(4, 6))
            
            self.is_logged_in = True
            return True
            
        except Exception as e:
            print(f"[Webmail] Generic login error: {e}")
            return False
    
    async def send_email(
        self,
        to_email: str,
        subject: str,
        body_html: str,
        body_text: Optional[str] = None
    ) -> Dict:
        """Send a single email via webmail"""
        
        # Validate email
        try:
            validate_email(to_email)
        except EmailNotValidError as e:
            return {'success': False, 'error': str(e)}
        
        # Get account
        if not self.is_logged_in:
            account = self.get_available_account()
            if not account:
                return {'success': False, 'error': 'No accounts available (rate limited)'}
            
            login_success = await self.login(account)
            if not login_success:
                return {'success': False, 'error': f'Failed to login to {account.email}'}
        
        try:
            print(f"[Webmail] Composing email to {to_email}...")
            
            # Click compose
            compose_selectors = [
                '#rcmbtn_compose',
                'a.compose',
                'button.compose',
                'a:has-text("Compose")',
                'a[href*="compose"]'
            ]
            
            for sel in compose_selectors:
                try:
                    if await self.page.is_visible(sel, timeout=2000):
                        await self.page.click(sel)
                        break
                except:
                    continue
            
            await asyncio.sleep(random.uniform(2, 4))
            
            # Fill To field
            to_selectors = [
                'input[name="_to"]',
                '#_to',
                'input[placeholder*="To" i]',
                '.input-to input'
            ]
            
            for sel in to_selectors:
                try:
                    if await self.page.is_visible(sel, timeout=2000):
                        await self.page.fill(sel, to_email)
                        break
                except:
                    continue
            
            await asyncio.sleep(random.uniform(1, 2))
            
            # Fill Subject
            subj_selectors = [
                'input[name="_subject"]',
                '#_subject',
                'input[placeholder*="Subject" i]'
            ]
            
            for sel in subj_selectors:
                try:
                    if await self.page.is_visible(sel, timeout=2000):
                        await self.page.fill(sel, subject)
                        break
                except:
                    continue
            
            await asyncio.sleep(random.uniform(1, 2))
            
            # Fill body
            body_text = body_text or self._html_to_text(body_html)
            
            body_selectors = [
                'textarea[name="_message"]',
                '#composebody',
                '.mceEditor textarea',
                '[contenteditable="true"]'
            ]
            
            for sel in body_selectors:
                try:
                    if await self.page.is_visible(sel, timeout=2000):
                        await self.page.fill(sel, body_text)
                        break
                except:
                    continue
            
            await asyncio.sleep(random.uniform(1, 3))
            
            # Click Send
            send_selectors = [
                'button[name="_send"]',
                'input[name="_send"]',
                'button.send',
                '#rcmbtn_send',
                'button:has-text("Send")'
            ]
            
            for sel in send_selectors:
                try:
                    if await self.page.is_visible(sel, timeout=2000):
                        await self.page.click(sel)
                        break
                except:
                    continue
            
            # Wait for send confirmation
            await asyncio.sleep(random.uniform(3, 5))
            
            # Check for success
            success = False
            success_indicators = [
                '.confirmation',
                '.success',
                '.alert-success',
                'text=Message sent',
                'text=Email sent'
            ]
            
            for indicator in success_indicators:
                try:
                    if await self.page.is_visible(indicator, timeout=3000):
                        success = True
                        break
                except:
                    continue
            
            # Also consider success if send buttons vanished
            if not success:
               try:
                   send_visible = await self.page.is_visible(send_selectors[0], timeout=1000)
                   if not send_visible:
                       success = True
               except:
                   success = True
            
            
            if success:
                # Update counters
                self.current_account.sent_today += 1
                self.current_account.sent_this_hour += 1
                
                print(f"[Webmail] ✓ Sent to {to_email}")
                return {
                    'success': True,
                    'from': self.current_account.email,
                    'to': to_email,
                    'sent_at': datetime.now().isoformat()
                }
            else:
                return {'success': False, 'error': 'Send confirmation not detected'}
                
        except Exception as e:
            print(f"[Webmail] Send error: {e}")
            return {'success': False, 'error': str(e)}
    
    async def send_campaign(
        self,
        recipients: List[Dict],
        subject_template: str,
        body_template: str,
        delay_range: Tuple[int, int] = (60, 180)
    ) -> Dict:
        """Send campaign to multiple recipients"""
        results = {
            'total': len(recipients),
            'sent': 0,
            'failed': 0,
            'errors': []
        }
        
        for i, recipient in enumerate(recipients):
            # Render templates
            subject = Template(subject_template).render(**recipient)
            body = Template(body_template).render(**recipient)
            
            # Send
            result = await self.send_email(
                to_email=recipient['email'],
                subject=subject,
                body_html=body,
                body_text=None
            )
            
            if result['success']:
                results['sent'] += 1
            else:
                results['failed'] += 1
                results['errors'].append(f"{recipient['email']}: {result.get('error')}")
            
            # Rate limiting delay
            if i < len(recipients) - 1:
                delay = random.randint(*delay_range)
                print(f"[Webmail] Waiting {delay}s before next email...")
                await asyncio.sleep(delay)
        
        return results
    
    @staticmethod
    def _html_to_text(html: str) -> str:
        """Simple HTML to text conversion"""
        text = re.sub(r'<br\s*/?>', '\n', html)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()


class WebmailTemplateLibrary:
    """Email templates optimized for webmail sending"""
    
    @staticmethod
    def get_templates() -> Dict:
        return {
            'intro_short': {
                'subject': 'Quick question about {{ company }}',
                'body': '''Hi {{ first_name }},

I came across {{ company }} and wanted to reach out. I'm a digital marketer who helps local businesses get more customers.

Are you currently running any paid advertising? If not, I'd love to share some ideas that could help {{ company }}.

Worth a quick chat?

Best,
{{ sender_name }}

---
{{ sender_company }}
{{ sender_phone }}'''
            },
            
            'follow_up': {
                'subject': 'Re: {{ company }}',
                'body': '''Hi {{ first_name }},

Following up on my last email about helping {{ company }} with marketing.

I know you're busy. If you're interested in seeing how we've helped similar businesses get more leads, just reply "YES" and I'll send over a case study.

If now's not the right time, no worries - just let me know.

Best,
{{ sender_name }}'''
            },
            
            'value_pitch': {
                'subject': 'How {{ company }} can get 30+ more leads/month',
                'body': '''Hi {{ first_name }},

Quick question: Is {{ company }} getting enough leads each month?

I help local businesses like yours get 30-50 qualified leads monthly through targeted Google ads and better website conversion.

For example, I recently helped a {{ industry }} business in {{ location }} increase their leads by 4x in just 60 days.

Worth a 10-minute call to see if I can do the same for {{ company }}?

Best,
{{ sender_name }}
{{ sender_email }}
{{ sender_phone }}

P.S. If you're not the right person for this, could you point me to who handles marketing?'''
            },
            
            'simple_follow_up': {
                'subject': '{{ company }} - Following up',
                'body': '''Hi {{ first_name }},

Just following up on my email about helping {{ company }} with digital marketing.

Still interested in learning how to get more customers? If yes, just reply and I'll send you some specific ideas.

If not, just let me know and I won't follow up again.

Thanks,
{{ sender_name }}'''
            }
        }


# ── Usage Example ─────────────────────────────────────────────

async def example_usage():
    """Example of how to use the webmail sender"""
    
    # Initialize sender
    sender = WebmailSender(headless=False)  # Set True for production
    await sender.start()
    
    # Add your HostFast webmail account
    # Replace with your actual details from hostfast.pk
    account = WebmailAccount(
        name="Your Name",
        email="you@yourdomain.com",  # Your email from hostfast
        password="your_webmail_password",
        webmail_url="https://webmail.yourdomain.com",  # Or the URL hostfast gives you
        provider="cpanel",
        daily_limit=100,  # Start conservative
        hourly_limit=15
    )
    
    sender.add_account(account)
    
    # Get templates
    templates = WebmailTemplateLibrary.get_templates()
    template = templates['intro_short']
    
    # Prepare recipients
    recipients = [
        {
            'email': 'lead1@example.com',
            'company': 'ABC Plumbing',
            'first_name': 'John',
            'location': 'Austin, TX',
            'sender_name': 'Your Name',
            'sender_company': 'Your Agency',
            'sender_phone': '555-1234'
        },
        {
            'email': 'lead2@example.com',
            'company': 'XYZ Services',
            'first_name': 'Mike',
            'location': 'Austin, TX',
            'sender_name': 'Your Name',
            'sender_company': 'Your Agency',
            'sender_phone': '555-1234'
        }
    ]
    
    # Send campaign
    results = await sender.send_campaign(
        recipients=recipients,
        subject_template=template['subject'],
        body_template=template['body'],
        delay_range=(90, 180)  # 1.5-3 minutes between emails
    )
    
    print(f"\nCampaign Results:")
    print(f"Sent: {results['sent']}/{results['total']}")
    print(f"Failed: {results['failed']}")
    
    await sender.close()


if __name__ == "__main__":
    asyncio.run(example_usage())
