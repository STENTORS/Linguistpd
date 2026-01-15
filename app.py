import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import os
import hmac
from dotenv import load_dotenv
import calendar
from dateutil import parser
import subprocess
import json
import sys

load_dotenv()


# =========== Sheet Connections ==========

social_conn = st.connection("social_gsheets", type=GSheetsConnection)
social_df = social_conn.read()

sales_conn = st.connection("sales_gsheets", type=GSheetsConnection)
sales_df = sales_conn.read(worksheet="Thinkifc orders updated by Zapier")

wp_sales_conn = st.connection("wp_sales_gsheets", type=GSheetsConnection)
wp_sales_df =wp_sales_conn.read()

email_conn = st.connection("email_gsheets", type=GSheetsConnection)
email_df = email_conn.read()

# =========== Password Check ==========
def check_password():
    def login_form():
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        if (st.secrets.login_credentials.admin_user == st.session_state['username'] and hmac.compare_digest(
                st.session_state["password"],
                st.secrets.login_credentials.admin_pass
            )):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    login_form()
    if "password_correct" in st.session_state:
        st.error("😕 Incorrect Admin Credentials")
    return False

# =========== Scraper Runner ==========
def run_scraper(command):
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            output = result.stdout
            # Attempt to parse number of new rows added
            new_rows = parse_new_rows_from_output(output)
            return new_rows
        else:
            raise Exception(f"Error: {result.stderr}")
    except Exception as e:
        st.error(f"Error running scraper: {str(e)}")
        return 0

def parse_new_rows_from_output(output):
    # Implement a way to parse the output for number of new rows (customize as needed)
    # For simplicity, let's assume the output contains a line like 'X new rows added.'
    lines = output.split('\n')
    for line in lines:
        if "new rows added" in line:
            parts = line.split()
            return int(parts[0])  # Assuming the first part is the number of new rows
    return 0

# =========== Streamlit Interface ==========
if check_password():
    st.title("Scraper Dashboard")
    st.write("Select a scraper to run:")

    if st.button("Run WP Scraper"):
        new_rows = run_scraper(["python3", "wp_scraper.py"])
        st.success(f"WP Scraper finished. {new_rows} new rows added.")

    if st.button("Run Email Data Scraper"):
        new_rows = run_scraper(["python3", "email_data.py"])
        st.success(f"Email Data Scraper finished. {new_rows} new rows added.")

    if st.button("Run Buffer Scraper"):
        new_rows = run_scraper(["python3", "buffer.py"])
        st.success(f"Buffer Scraper finished. {new_rows} new rows added.")

# =========== The existing Data Processing and Analysis Functions remain unchanged ===========

# The remaining functions should be included here unchanged from the original app.py.

