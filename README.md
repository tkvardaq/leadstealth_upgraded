# LeadStealth Webmail Edition

**Free email sending using your HostFast hosting webmail!**

Your hosting (hostfast.pk) includes free webmail - this tool automates it to send your marketing emails at zero extra cost.

---

## 📦 What's Included

### 1. **Webmail Sender** (`webmail/sender.py`)
- **Auto-logs into your webmail** (cPanel/RoundCube)
- **Sends emails just like you would manually**
- **Rotates between multiple accounts** if you have them
- **Rate limiting** - respects your host's limits (default: 100/day, 15/hour)
- **4 pre-built email templates** ready to use

### 2. **Webmail Dashboard** (`app_webmail.py`)
- **Streamlit UI** - same as your current tool
- **Lead scraping** from Google Maps & Yellow Pages
- **Email campaign management**
- **Track sent/replied/bounced**
- **No code needed** - all point-and-click

### 3. **Email Templates**
- `intro_short` - Soft intro, asks about current marketing
- `follow_up` - Follow-up with case study offer
- `value_pitch` - Direct value pitch with examples
- `simple_follow_up` - Short, casual follow-up

---

## 🚀 Setup Instructions

### Step 1: Get Your Webmail URL

1. Log into your HostFast cPanel
2. Find **"Email Accounts"** or **"Webmail"**
3. Click **"Access Webmail"** or **"Check Email"**
4. Look at the URL - it will be something like:
   - `https://webmail.yourdomain.com`
   - `https://yourdomain.com:2096`
   - `https://yourdomain.com/webmail`

### Step 2: Install

```bash
# Copy your old scraper files (already done)
# scraper.py, db.py, processor.py, run_scraper_cli.py

# Install dependencies
pip install -r requirements-webmail.txt

# Install Playwright browsers
playwright install chromium
```

### Step 3: Run

```bash
streamlit run app_webmail.py
```

---

## 📧 How It Works

1. **You configure your webmail account** in the sidebar
2. **Scrape leads** (Google Maps / Yellow Pages)
3. **Select leads** with emails
4. **Choose a template** or write custom
5. **Click "Start Campaign"**
6. **Browser opens** and logs into webmail automatically
7. **Emails are sent one by one** with human-like delays
8. **Watch it work** - browser visible so you can see progress

---

## ⚙️ Webmail Settings for HostFast

**Typical settings:**
- **Webmail URL**: `https://webmail.yourdomain.com` or `https://yourdomain.com:2096`
- **Provider**: `cpanel` (most common)
- **Daily Limit**: 100 (start conservative, can increase to 250)
- **Hourly Limit**: 15-20

**Important**: Start with low limits and increase gradually to avoid your host flagging you.

---

## 🛡️ Safety Features

- **Delays between emails** (90-180 seconds by default)
- **Daily/hourly rate limits** per account
- **Account rotation** if you add multiple
- **Human-like behavior** - typing delays, random pauses
- **No bulk sending** - one at a time like a real person

---

## 📊 Campaign Workflow

```
1. Scrape leads → 50 plumbers in Austin
2. Filter → Only those with emails (maybe 20)
3. Template → "intro_short"
4. Settings → 10 emails, 90-180s delays
5. Launch → Browser opens, sends 1 by 1
6. Results → Track in Analytics tab
```

---

## 💡 Pro Tips

1. **Start small** - Send 10-20 emails first to test
2. **Warm up** - Send 5/day for a few days, then increase
3. **Use multiple accounts** - Create 2-3 email accounts on your host
4. **Personalize templates** - Edit them to match your voice
5. **Check spam** - Test send to your own Gmail first

---

## 🔧 Customization

### Add Your Own Template

Edit `webmail/sender.py` in the `WebmailTemplateLibrary.get_templates()`:

```python
'my_custom': {
    'subject': 'Question about {{ company }}',
    'body': '''Hi {{ first_name }},

[Your custom message here]

Best,
{{ sender_name }}'''
}
```

**Variables available:**
- `{{ company }}` - Business name
- `{{ first_name }}` - Extracted first name
- `{{ location }}` - Location from search
- `{{ sender_name }}` - Your name
- `{{ sender_company }}` - Your agency name
- `{{ sender_email }}` - Your email
- `{{ sender_phone }}` - Your phone

---

## 🐛 Troubleshooting

### "Can't login"
- Double-check your webmail URL
- Try logging in manually first
- Check if 2FA is enabled (disable it)

### "Emails not sending"
- Check if browser window opens
- Look for CAPTCHA - solve manually if needed
- Lower your hourly limit

### "Host blocked me"
- Wait 24 hours
- Lower your daily limit
- Contact HostFast support

### "Browser doesn't open"
- Make sure Playwright is installed: `playwright install chromium`
- Try with `headless=False` in the code

---

## 📁 File Structure

```
leadstealth_upgraded/
├── app_webmail.py              # Main Streamlit app
├── webmail/
│   └── sender.py               # Webmail automation
├── scraper.py                  # Your Google Maps/YP scraper
├── db.py                       # Your CSV database
├── processor.py                # Lead enrichment
├── run_scraper_cli.py          # CLI scraper runner
├── requirements-webmail.txt    # Dependencies
└── leads.db.csv               # Your leads (auto-created)
```

---

## ✅ Next Steps

1. **Test with 5 emails** to yourself/Gmail
2. **Check they arrive** in inbox (not spam)
3. **Adjust template** if needed
4. **Send to 10 real leads**
5. **Scale up gradually**

---

**Questions?** The browser stays visible during sending so you can see what's happening. If something breaks, you'll see the error in the browser window.

Good luck with your campaigns! 🚀
