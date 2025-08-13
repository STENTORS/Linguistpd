import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import os
import hmac
from dotenv import load_dotenv
load_dotenv()

#===========Sheet Connections===========
# Connect to the public sheet
social_conn = st.connection("social_gsheets", type=GSheetsConnection)
social_df = social_conn.read()

# Connect to the private sheet
sales_conn = st.connection("sales_gsheets", type=GSheetsConnection)
sales_df = sales_conn.read()

def normalize_dates(date_str):
    date_str = str(date_str).strip()
    today = datetime.today().date()
    
    if date_str.lower().startswith("yesterday"):
        # e.g. "Yesterday, 8 August"
        # Just return yesterday's date
        return today - timedelta(days=1)
    elif date_str.lower().startswith("today"):
        return today
    else:
        # Try normal parsing for the rest
        try:
            return pd.to_datetime(date_str, dayfirst=True).date()
        except Exception:
            return None


def check_password():
    """Returns `True` if the user had a correct password."""

    def login_form():
        """Form to collect username & password"""
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if os.environ.get(f"{st.session_state["username"].upper()}_STREAMLIT_PASSWORD") \
            and hmac.compare_digest(
            st.session_state["password"],
            os.environ.get(f"{st.session_state["username"].upper()}_STREAMLIT_PASSWORD")
        ):
            st.session_state["password_correct"] = True
            # Don't store the username or password in the session
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    # Return True if the username + password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show inputs for username + password.
    login_form()
    if "password_correct" in st.session_state:
        st.error("😕 Incorrrect Admin Credentials")
    return False


#===========Main Application===========
social_df["Date"] = social_df["Date"].apply(normalize_dates)

#========Header========
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Admin Dashboard")

#========login system========
if not check_password():
    st.stop()

# ========Content========
tab_main, tab_sales, tab_social, tab_email = st.tabs(["📈 Analytics", "Sales Data", "Social Data", "Email Marketing"])

with st.sidebar:
    st.button("Reload Scraper")
    st.button("Customer Sales Tracker")
    st.button("Amalytics")
    st.button("raw data")

with tab_main:

    st.header("Sales x Post by Date")

    #==============Graph Options==============
    #  Platform selection pills
    platforms = ["Sale Period", "LinkedIn", "Twitter", "Facebook"]

    selected_platforms = st.multiselect(
        "Graph View Options",
        platforms,
        default=platforms 
    )

    #==============Graph View==============
    # Parse dates
    sales_df["Date and Time"] = pd.to_datetime(sales_df["Date and Time"], errors="coerce")
    sales_df["Date"] = sales_df["Date and Time"].dt.date

    # Date range selector
    min_date = sales_df["Date"].min()
    max_date = sales_df["Date"].max()
    start_date, end_date = st.date_input(
        "Select date range:",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    # Filter by range
    sales_filtered = sales_df[
        (sales_df["Date"] >= start_date) & (sales_df["Date"] <= end_date)
    ]

    # Aggregate sales count per year
    sales_yearly = sales_filtered.groupby(
        sales_filtered["Date and Time"].dt.month
    ).size().reset_index(name="Sales Count")

    # Chart
    chart = alt.Chart(sales_yearly).mark_line(point=True).encode(
        x=alt.X("Date and Time:O", title="Month"),
        y=alt.Y("Sales Count:Q", title="Number of Sales"),
        tooltip=["Date and Time", "Sales Count"]
    ).properties(
        title="Yearly Sales Count"
    )

    st.altair_chart(chart, use_container_width=True)

with tab_sales:
    st.header("Sales Data")
    st.dataframe(sales_df)

with tab_social:
    st.header("Social Media Data")
    st.dataframe(social_df)

with  tab_email:
    st.header("Email Markting Data")


st.divider()
st.button("Run Scraper")
