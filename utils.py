import os
import subprocess
import sys
from pathlib import Path

def install_playwright():
    """
    Install Playwright browsers if they are missing.
    Used for Streamlit Cloud and local first-time setup.
    """
    # Use a marker in home directory to avoid repeated checks
    marker = Path.home() / ".playwright_installed"
    
    # Also check if it's already installed in common locations to be extra sure
    # On Streamlit Cloud this is usually /home/adminuser/.cache/ms-playwright
    if not marker.exists():
        try:
            print("[LeadStealth] Checking for browser engines...")
            # We use chromium as it's the most efficient for our needs
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
            marker.touch()
            print("[LeadStealth] Browser engines are ready.")
        except Exception as e:
            print(f"[LeadStealth] Browser installation failed: {e}")
