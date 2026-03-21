# Diagnostic Entrypoint for LeadStealth
import streamlit as st
import sys
import os
import time

# ── Early Logging ──
print(f"[LeadStealth Entry] Process started: {time.ctime()}")
print(f"[LeadStealth Entry] Python: {sys.version}")
print(f"[LeadStealth Entry] CWD: {os.getcwd()}")

# ── Safe Import ──
try:
    print("[LeadStealth Entry] Attempting to import app_webmail...")
    import app_webmail
    print("[LeadStealth Entry] app_webmail imported successfully.")
except Exception as e:
    print(f"[LeadStealth Entry] CRASH DURING IMPORT: {e}")
    st.error(f"Critical Startup Error: {e}")
    st.info("This is likely a dependency conflict or a namespace shadowing issue.")
    st.stop()

# ── If we get here, just let the app run ──
# (Streamlit handles the rest)
