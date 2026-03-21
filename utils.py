import os
import subprocess
import sys
from pathlib import Path

def install_playwright():
    """
    Install Playwright browsers if they are missing.
    Used for Streamlit Cloud and local first-time setup.
    """
    # Use a marker in /tmp directory to avoid repeated checks (guaranteed writeable)
    marker = Path("/tmp/.playwright_installed")
    
    # Also check if it's already installed in common locations to be extra sure
    if not marker.exists():
        try:
            print("[LeadStealth] Checking for browser engines...")
            # We use chromium as it's the most efficient for our needs
            # Add --with-deps as a safeguard, and ensure non-interactive
            print("[LeadStealth] Running: playwright install chromium")
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True, capture_output=True, text=True)
            marker.touch()
            print("[LeadStealth] Browser engines are ready.")
        except subprocess.CalledProcessError as e:
            print(f"[LeadStealth] Browser installation failed: {e}")
            if e.stderr:
                print(f"[LeadStealth] Error detail: {e.stderr}")
        except Exception as e:
            print(f"[LeadStealth] Unexpected error during installation: {e}")
    else:
        # Check if actually exists even if marker exists
        print("[LeadStealth] Browser marker found, setup already completed.")
