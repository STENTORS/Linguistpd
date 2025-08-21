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

load_dotenv()

# =========== Sheet Connections ==========
social_conn = st.connection("social_gsheets", type=GSheetsConnection)
social_df = social_conn.read()

sales_conn = st.connection("sales_gsheets", type=GSheetsConnection)
sales_df = sales_conn.read()

wp_sales_conn = st.connection("wp_sales_gsheets", type=GSheetsConnection)
wp_sales_df = wp_sales_conn.read()

# =========== Password Check ==========
def check_password():
    def login_form():
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        if os.environ.get(f"{st.session_state['username'].upper()}_STREAMLIT_PASSWORD") \
            and hmac.compare_digest(
                st.session_state["password"],
                os.environ.get(f"{st.session_state['username'].upper()}_STREAMLIT_PASSWORD")
            ):
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

# =========== Date Parsing Functions ==========
def parse_social_date(date_str):
    """Parse social media date strings like 'Today, 15 August' into datetime objects"""
    try:
        # Handle "Today" and "Yesterday" cases
        today = datetime.now().date()
        if date_str.startswith("Today"):
            return today
        elif date_str.startswith("Yesterday"):
            return today - timedelta(days=1)
        
        # Handle day names (e.g., "Tuesday, 12 August")
        date_str = date_str.split(", ")[-1]  # Get the "15 August" part
        return parser.parse(date_str).date()
    except:
        return None

# =========== Data Processing Functions ==========
def prepare_sales_data(df):
    """Process sales data and aggregate by month"""
    df["Date and Time"] = pd.to_datetime(df["Date and Time"], errors="coerce")
    df["Year"] = df["Date and Time"].dt.year
    df["Month"] = df["Date and Time"].dt.month
    
    # Aggregate sales by month
    monthly_sales = df.groupby(["Year", "Month"]).agg({
        "Amount": "sum"
    }).reset_index()
    
    # Create proper date column for plotting (first day of each month)
    monthly_sales["Date"] = pd.to_datetime(
        monthly_sales[["Year", "Month"]].assign(DAY=1)
    )
    monthly_sales["Month_Name"] = monthly_sales["Month"].apply(
        lambda x: calendar.month_abbr[x]
    )
    return monthly_sales

def prepare_social_data(df):
    """Process social data and aggregate by month"""
    # Parse dates
    df["Parsed_Date"] = df["Date"].apply(parse_social_date)
    df = df.dropna(subset=["Parsed_Date"])
    df["Parsed_Date"] = pd.to_datetime(df["Parsed_Date"])
    
    # Extract year and month
    df["Year"] = df["Parsed_Date"].dt.year
    df["Month"] = df["Parsed_Date"].dt.month
    
    # Define engagement columns
    engagement_columns = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    
    # Clean and convert engagement data
    for col in engagement_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].apply(
                lambda x: 0 if "no data available" in x.lower() else x
            )
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df["Total_Score"] = df[engagement_columns].sum(axis=1)
    
    # Aggregate by month
    monthly_social = df.groupby(["Year", "Month"]).agg({
        "Total_Score": "sum"
    }).reset_index()
    
    # Create proper date column for plotting (first day of each month)
    monthly_social["Date"] = pd.to_datetime(
        monthly_social[["Year", "Month"]].assign(DAY=1)
    )
    monthly_social["Month_Name"] = monthly_social["Month"].apply(
        lambda x: calendar.month_abbr[x]
    )
    return monthly_social

# =========== Main Application ==========
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Admin Dashboard")

# Login system
if not check_password():
    st.stop()

# Process data
monthly_sales = prepare_sales_data(sales_df)
monthly_social = prepare_social_data(social_df)

# Get available years from data
available_years = sorted(
    set(monthly_sales["Year"].unique()).union(
        set(monthly_social["Year"].unique())
    )
)
selected_year = st.selectbox(
    "Select Year", 
    available_years, 
    index=len(available_years)-1
)

# Filter data by selected year
sales_filtered = monthly_sales[monthly_sales["Year"] == selected_year]
social_filtered = monthly_social[monthly_social["Year"] == selected_year]

# Combine data for shared x-axis
combined_data = pd.concat([
    sales_filtered.assign(Type="Sales"),
    social_filtered.assign(Type="Social")
])

# =========== Content ==========
tab_main, tab_sales, tab_social, tab_email, tab_payment_count = st.tabs([
    "📈 Analytics", "Sales Data", "Social Data", "Email Marketing", "User Offers"
])

with tab_main:
    st.header(f"Sales vs Social Performance ({selected_year})")
    
    if not combined_data.empty:
        # Create base chart with shared x-axis
        base = alt.Chart(combined_data).encode(
            x=alt.X('month(Date):T', title='Month', axis=alt.Axis(format='%b'))
        )
        
        # Sales line
        sales_line = base.transform_filter(
            alt.datum.Type == "Sales"
        ).mark_line(color='blue').encode(
            y=alt.Y('Amount:Q', title='Sales Amount', scale=alt.Scale(zero=False)),
            tooltip=['Month_Name', 'Year', 'Amount']
        )
        
        # Social scatter
        social_scatter = base.transform_filter(
            alt.datum.Type == "Social"
        ).mark_circle(color='red', size=60).encode(
            y=alt.Y('Total_Score:Q', title='Social Score', scale=alt.Scale(zero=False)),
            tooltip=['Month_Name', 'Year', 'Total_Score']
        )
        
        # Combine charts
        combined_chart = alt.layer(sales_line, social_scatter).resolve_scale(
            y='independent'
        ).properties(
            width=800,
            height=400
        )
        
        st.altair_chart(combined_chart, use_container_width=True)
        
        # Metrics
        col1, col2 = st.columns(2)
        with col1:
            total_sales = sales_filtered["Amount"].sum()
            st.metric("Total Sales", f"${total_sales:,.2f}")
        
        with col2:
            total_social = social_filtered["Total_Score"].sum()
            st.metric("Total Social Score", f"{total_social:,.0f}")
    else:
        st.warning("No data available for the selected year.")

with tab_sales:
    st.header("Sales Data")
    st.dataframe(sales_df)

    st.header("WP Sales Data")
    st.dataframe(wp_sales_df)
    
with tab_social:
    st.header("Social Media Data")
    st.dataframe(social_df)

with tab_email:
    st.header("Email Marketing Data")

with tab_payment_count:
    st.header("Sales per user count")

st.divider()
st.button("Run Scrapers")#buffer + wp-scraper - so i need to automate authentation