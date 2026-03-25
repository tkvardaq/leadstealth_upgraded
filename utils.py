import os
import subprocess
import sys
import tempfile
from pathlib import Path


def install_playwright():
    """
    Install Playwright browsers if they are missing.
    Used for Streamlit Cloud and local first-time setup.
    """
    # Use cross-platform temp directory
    marker = Path(tempfile.gettempdir()) / ".playwright_installed"

    if not marker.exists():
        try:
            print("[LeadStealth] Running: playwright install chromium")
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
                capture_output=True,
                text=True,
            )
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
            print("[LeadStealth] Browser engines ready.")
        except subprocess.CalledProcessError as e:
            print(f"[LeadStealth] Browser install failed: {e}")
            if e.stderr:
                print(f"[LeadStealth] Detail: {e.stderr}")
        except Exception as e:
            print(f"[LeadStealth] Install error: {e}")
