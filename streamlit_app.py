# LeadStealth Entry Point
import streamlit as st

st.set_page_config(
    page_title="LeadStealth Webmail Edition", page_icon="📧", layout="wide"
)

try:
    import app_webmail
except Exception as e:
    st.error(f"Startup Error: {e}")
    st.stop()
