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

def clean_wp_date(date_str):
    try:
        date = date_str.split()[1]
        time = date_str.split()[-1]
        datetime = date + " " + time
        datetime = pd.to_datetime(datetime)
        return datetime
    except:
        return None
# =========== Data Processing Functions ==========
def prepare_sales_data(df):
    """Process sales data and aggregate by month"""
    df["Date"] = pd.to_datetime(df["Date and Time"], errors="coerce")
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

def prepare_wp_sales_data(df):
    """Proccess live webinar sales data - get month"""
    df["cleaned date"] = df["Date"].apply(clean_wp_date)
    df["Date"] = df["cleaned date"].dt.date
    df["Month"] = df["cleaned date"].dt.month
    df["Year"] = df["cleaned date"].dt.year

    monthly_wp_sales = df.groupby(["Year", "Month"]).agg({
        "Total Amount": "sum"
    }).reset_index()

    monthly_wp_sales["Date"] = pd.to_datetime(
        monthly_wp_sales[["Year", "Month"]].assign(DAY=1)
    )

    monthly_wp_sales["Month_Name"] = monthly_wp_sales["Month"].apply(
        lambda x: calendar.month_abbr[x]
    )

    return monthly_wp_sales


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

# ====================== Main Application =====================
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Admin Dashboard")

# Login system
#    if not check_password():
#        st.stop()

# ====================== Line Graph setup =====================

# formating the dfs for the chart
monthly_sales = prepare_sales_data(sales_df)
monthly_wp_sales = prepare_wp_sales_data(wp_sales_df)
monthly_social = prepare_social_data(social_df)

# Get available years from all the dfs
available_years = sorted(
    set(monthly_sales["Year"].unique()).union(
        set(monthly_social["Year"].unique()),
        set(monthly_wp_sales["Year"].unique())
    )
)

selected_year = st.selectbox(
    "Select Year", 
    available_years, 
    index=len(available_years)-1
)

# Filter data by selected year
sales_filtered = monthly_sales[monthly_sales["Year"] == selected_year]
wp_sales_filtered = monthly_wp_sales[monthly_wp_sales["Year"] == selected_year]
social_filtered = monthly_social[monthly_social["Year"] == selected_year]

# Combine data for shared x-axis
combined_data = pd.concat([
    sales_filtered.assign(Type="Sales"),
    wp_sales_filtered.assign(Type="Live"),
    social_filtered.assign(Type="Social")
])

# ====================== Content =====================
tab_main, tab_sales, tab_social, tab_email, tab_payment_count = st.tabs([
    "📈 Analytics", "Sales Data", "Social Data", "Email Marketing", "Sales by User"
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

        wp_sales_line = base.transform_filter(
            alt.datum.Type == "Live"
        ).mark_line(color='green').encode(
            y=alt.Y('Total Amount:Q', scale=alt.Scale(zero=False)),
            tooltip=['Month_Name', 'Year', 'Total Amount']
        )
        
        # Social scatter
        social_scatter = base.transform_filter(
            alt.datum.Type == "Social"
        ).mark_circle(color='red', size=60).encode(
            y=alt.Y('Total_Score:Q', title='Social Score', scale=alt.Scale(zero=False)),
            tooltip=['Month_Name', 'Year', 'Total_Score']
        )

        # Combine charts
        combined_chart = alt.layer(sales_line, wp_sales_line, social_scatter).resolve_scale(
            y='shared'
        ).properties(
            width=800,
            height=400
        )
        
        st.altair_chart(combined_chart, use_container_width=True)
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            total_sales = sales_filtered["Amount"].sum() + wp_sales_filtered["Total Amount"].sum()
            st.metric("Total Sales", f"£{total_sales:,.2f}")
        
        with col2:
            st.metric("Total Live Webinar Sales", f"£{wp_sales_filtered["Total Amount"].sum():,.0f}")
        with col3:
            st.metric("Total Pre Recorded Sales", f"£{sales_filtered["Amount"].sum():,.0f}")

        st.divider()
        st.header("Analytics")
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
    st.dataframe(email_df)

with tab_payment_count:
    st.header("Sales by User")

    group = sales_df.groupby("Email address")
    total_amount = group["Amount"].sum().reset_index(name="Amount")
    purchase_counts = sales_df["Email address"].value_counts().reset_index(name = "Purchase Count")
    purchase_counts["Amount Spent"] = total_amount["Amount"]
    purchase_counts.columns = ["Email address", "Purchase Count", "Amount Spent"]
    
    wp_group = wp_sales_df.groupby("Email address")
    total_wp_amount = wp_group["Total Amount"].sum().reset_index(name="Total Amount")

    purchase_counts_wp = wp_sales_df["Email address"].value_counts().reset_index()
    purchase_counts_wp["Amount Spent"] = total_wp_amount["Total Amount"]
    purchase_counts_wp.columns = ["Email address", "Purchase Count", "Amount Spent"]

    st.subheader("Thinkific Sales")
    st.write(purchase_counts)
    st.subheader("Live Webinar Sales")
    st.write(purchase_counts_wp)

    #combine counts
    st.subheader("Combined Data")
    combine_counts = pd.concat([purchase_counts, purchase_counts_wp], ignore_index=True)
    combined_group = combine_counts.groupby("Email address")
    total_group = combined_group["Purchase Count"].sum().reset_index(name = "Purchase Count")
    total_group_amount = combined_group["Amount Spent"].sum().reset_index(name = "Amount Spent")
    combine_counts["Purchase Count"] = total_group["Purchase Count"]
    combine_counts["Amount Spent"] = total_group_amount["Amount Spent"]

    

    st.write(combine_counts)

st.divider()
st.button("Run Scrapers")#buffer + wp-scraper - so i need to automate authentation