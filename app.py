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
    """Clean WordPress sales dates with better error handling"""
    try:
        if pd.isna(date_str) or date_str == "":
            return None
        
        # Try multiple date parsing strategies
        if isinstance(date_str, str):
            # Remove any extra spaces and try parsing
            date_str = ' '.join(date_str.split())
            
            # Try direct parsing first
            try:
                return pd.to_datetime(date_str)
            except:
                pass
            
            # Try splitting by space and taking relevant parts
            parts = date_str.split()
            if len(parts) >= 2:
                # Take date part and time part
                date_part = parts[0] if len(parts[0].split('-')) == 3 else parts[1]
                time_part = parts[-1] if ':' in parts[-1] else '00:00:00'
                
                datetime_str = f"{date_part} {time_part}"
                return pd.to_datetime(datetime_str)
        
        return pd.to_datetime(date_str)
    except:
        return None

# =========== Data Processing Functions ==========
def prepare_daily_sales_data(df):
    """Process sales data and aggregate by day"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    df_clean["Date"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    df_clean["Date and Time"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=["Date and Time"])
    
    df_clean["Year"] = df_clean["Date and Time"].dt.year
    df_clean["Month"] = df_clean["Date and Time"].dt.month
    df_clean["Day"] = df_clean["Date and Time"].dt.day
    
    # Aggregate sales by day
    daily_sales = df_clean.groupby(["Year", "Month", "Day"]).agg({
        "Amount": "sum"
    }).reset_index()
    
    # Create proper date column for plotting
    daily_sales["Date"] = pd.to_datetime(
        daily_sales[["Year", "Month", "Day"]]
    )
    return daily_sales

def prepare_daily_wp_sales_data(df):
    """Process live webinar sales data by day"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    df_clean["cleaned date"] = df_clean["Date"].apply(clean_wp_date)
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=["cleaned date"])
    
    df_clean["Year"] = df_clean["cleaned date"].dt.year
    df_clean["Month"] = df_clean["cleaned date"].dt.month
    df_clean["Day"] = df_clean["cleaned date"].dt.day

    daily_wp_sales = df_clean.groupby(["Year", "Month", "Day"]).agg({
        "Total Amount": "sum"
    }).reset_index()

    daily_wp_sales["Date"] = pd.to_datetime(
        daily_wp_sales[["Year", "Month", "Day"]]
    )

    return daily_wp_sales

def prepare_daily_social_data(df):
    """Process social data and aggregate by day"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse dates
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    
    # Extract year, month, day
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    df_clean["Day"] = df_clean["Parsed_Date"].dt.day
    
    # Define engagement columns
    engagement_columns = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    
    # Clean and convert engagement data
    for col in engagement_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].apply(
                lambda x: 0 if "no data available" in x.lower() else x
            )
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    
    df_clean["Total_Score"] = df_clean[engagement_columns].sum(axis=1)
    
    # Aggregate by day
    daily_social = df_clean.groupby(["Year", "Month", "Day"]).agg({
        "Total_Score": "sum"
    }).reset_index()
    
    # Create proper date column for plotting
    daily_social["Date"] = pd.to_datetime(
        daily_social[["Year", "Month", "Day"]]
    )
    return daily_social

# Keep the monthly functions for analysis (with error handling)
def prepare_sales_data(df):
    """Process sales data and aggregate by month"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    df_clean["Date"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    df_clean["Date and Time"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=["Date and Time"])
    
    df_clean["Year"] = df_clean["Date and Time"].dt.year
    df_clean["Month"] = df_clean["Date and Time"].dt.month

    # Aggregate sales by month
    monthly_sales = df_clean.groupby(["Year", "Month"]).agg({
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
    """Process live webinar sales data - get month"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    df_clean["cleaned date"] = df_clean["Date"].apply(clean_wp_date)
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=["cleaned date"])
    
    df_clean["Year"] = df_clean["cleaned date"].dt.year
    df_clean["Month"] = df_clean["cleaned date"].dt.month

    monthly_wp_sales = df_clean.groupby(["Year", "Month"]).agg({
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
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse dates
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    
    # Extract year and month
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    
    # Define engagement columns
    engagement_columns = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    
    # Clean and convert engagement data
    for col in engagement_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].apply(
                lambda x: 0 if "no data available" in x.lower() else x
            )
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    
    df_clean["Total_Score"] = df_clean[engagement_columns].sum(axis=1)
    
    # Aggregate by month
    monthly_social = df_clean.groupby(["Year", "Month"]).agg({
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

# =========== Analysis Functions ==========
def analyze_best_posting_times(social_df, sales_df, wp_sales_df):
    """Analyze data to recommend best posting times"""
    recommendations = []
    
    # Analyze social engagement patterns
    try:
        social_df_clean = social_df.copy()
        social_df_clean["Parsed_Date"] = social_df_clean["Date"].apply(parse_social_date)
        social_df_clean = social_df_clean.dropna(subset=["Parsed_Date"])
        social_df_clean["Parsed_Date"] = pd.to_datetime(social_df_clean["Parsed_Date"])
        social_df_clean["Month"] = social_df_clean["Parsed_Date"].dt.month
        social_df_clean["DayOfWeek"] = social_df_clean["Parsed_Date"].dt.day_name()
        
        # Find best performing months for social
        monthly_social = social_df_clean.groupby("Month").agg({
            "Total_Score": "mean"
        }).reset_index()
        if not monthly_social.empty:
            best_social_month = monthly_social.loc[monthly_social["Total_Score"].idxmax(), "Month"]
            best_month_name = calendar.month_name[best_social_month]
            recommendations.append(f"📈 **Best social media month**: {best_month_name} (highest engagement)")
    except:
        recommendations.append("📈 Social media data analysis unavailable")
    
    # Analyze sales patterns
    try:
        # Combine both sales datasets
        sales_df_clean = sales_df.copy()
        wp_sales_df_clean = wp_sales_df.copy()
        
        sales_df_clean["Date"] = pd.to_datetime(sales_df_clean["Date and Time"], errors="coerce")
        wp_sales_df_clean["cleaned date"] = wp_sales_df_clean["Date"].apply(clean_wp_date)
        
        # Remove invalid dates
        sales_df_clean = sales_df_clean.dropna(subset=["Date"])
        wp_sales_df_clean = wp_sales_df_clean.dropna(subset=["cleaned date"])
        
        all_sales = pd.concat([
            sales_df_clean[["Date", "Amount"]].rename(columns={"Amount": "Value"}),
            wp_sales_df_clean[["cleaned date", "Total Amount"]].rename(columns={"cleaned date": "Date", "Total Amount": "Value"})
        ])
        
        all_sales = all_sales.dropna(subset=["Date"])
        all_sales["Month"] = all_sales["Date"].dt.month
        
        monthly_sales = all_sales.groupby("Month").agg({
            "Value": "sum"
        }).reset_index()
        
        if not monthly_sales.empty:
            best_sales_month = monthly_sales.loc[monthly_sales["Value"].idxmax(), "Month"]
            best_sales_month_name = calendar.month_name[best_sales_month]
            recommendations.append(f"💰 **Best sales month**: {best_sales_month_name} (highest revenue)")
    except:
        recommendations.append("💰 Sales pattern analysis unavailable")
    
    # Correlation analysis
    try:
        # Prepare monthly data for correlation
        monthly_social_agg = prepare_social_data(social_df)
        monthly_sales_agg = prepare_sales_data(sales_df)
        monthly_wp_sales_agg = prepare_wp_sales_data(wp_sales_df)
        
        # Merge data for correlation analysis
        merged_data = monthly_social_agg.merge(
            monthly_sales_agg[["Year", "Month", "Amount"]], 
            on=["Year", "Month"], 
            how="inner", 
            suffixes=('_social', '_sales')
        )
        
        if len(merged_data) > 1:
            correlation = merged_data["Total_Score"].corr(merged_data["Amount"])
            if correlation > 0.5:
                recommendations.append("🔗 **Strong positive correlation** between social engagement and sales")
            elif correlation > 0.2:
                recommendations.append("🔗 **Moderate correlation** between social engagement and sales")
            else:
                recommendations.append("🔗 **Weak correlation** between social engagement and sales")
    except:
        recommendations.append("🔗 Correlation analysis unavailable")
    
    # Peak performance recommendations
    recommendations.extend([
        "🕒 **Recommended posting times**:",
        "   • **Tuesday-Thursday**: 9 AM - 12 PM (highest professional engagement)",
        "   • **Lunch hours**: 12 PM - 2 PM (mobile device usage peaks)",
        "   • **Evening slots**: 7 PM - 9 PM (leisure browsing time)"
    ])
    
    return recommendations

def create_performance_metrics(sales_df, wp_sales_df, social_df):
    """Create key performance metrics"""
    metrics = {}
    
    try:
        # Total revenue
        thinkific_sales = sales_df["Amount"].sum() if "Amount" in sales_df.columns else 0
        webinar_sales = wp_sales_df["Total Amount"].sum() if "Total Amount" in wp_sales_df.columns else 0
        metrics["total_revenue"] = thinkific_sales + webinar_sales
        metrics["thinkific_revenue"] = thinkific_sales
        metrics["webinar_revenue"] = webinar_sales
        
        # Social engagement
        if "Total_Score" in social_df.columns:
            metrics["avg_engagement"] = social_df["Total_Score"].mean()
            metrics["max_engagement"] = social_df["Total_Score"].max()
        else:
            engagement_cols = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
            if any(col in social_df.columns for col in engagement_cols):
                available_cols = [col for col in engagement_cols if col in social_df.columns]
                social_df_clean = social_df.copy()
                social_df_clean["Engagement_Score"] = social_df_clean[available_cols].sum(axis=1)
                metrics["avg_engagement"] = social_df_clean["Engagement_Score"].mean()
                metrics["max_engagement"] = social_df_clean["Engagement_Score"].max()
        
        # Growth metrics (simplified)
        metrics["total_customers"] = len(sales_df) + len(wp_sales_df)
        
    except Exception as e:
        st.error(f"Error calculating metrics: {e}")
    
    return metrics

# ====================== Main Application =====================
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Admin Dashboard")

# Login system
#    if not check_password():
#        st.stop()

# ====================== Daily Graph setup =====================

try:
    # Prepare daily data for the chart
    daily_sales = prepare_daily_sales_data(sales_df)
    daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
    daily_social = prepare_daily_social_data(social_df)

    # Prepare monthly data for analysis (keeping existing functionality)
    monthly_sales = prepare_sales_data(sales_df)
    monthly_wp_sales = prepare_wp_sales_data(wp_sales_df)
    monthly_social = prepare_social_data(social_df)

    # Get available years from all the dfs
    available_years = sorted(
        set(daily_sales["Year"].unique()).union(
            set(daily_social["Year"].unique()),
            set(daily_wp_sales["Year"].unique())
        )
    )

    selected_year = st.selectbox(
        "Select Year", 
        available_years, 
        index=len(available_years)-1 if available_years else 0
    )

    # Filter daily data by selected year
    daily_sales_filtered = daily_sales[daily_sales["Year"] == selected_year]
    daily_wp_sales_filtered = daily_wp_sales[daily_wp_sales["Year"] == selected_year]
    daily_social_filtered = daily_social[daily_social["Year"] == selected_year]

    # Filter monthly data by selected year (for analysis)
    sales_filtered = monthly_sales[monthly_sales["Year"] == selected_year]
    wp_sales_filtered = monthly_wp_sales[monthly_wp_sales["Year"] == selected_year]
    social_filtered = monthly_social[monthly_social["Year"] == selected_year]

    # Combine daily data for shared x-axis
    combined_daily_data = pd.concat([
        daily_sales_filtered.assign(Type="Sales"),
        daily_wp_sales_filtered.assign(Type="Live"),
        daily_social_filtered.assign(Type="Social")
    ])

except Exception as e:
    st.error(f"Error processing data: {e}")
    st.info("Please check your data sources and try again.")
    # Set default values to prevent further errors
    available_years = [datetime.now().year]
    selected_year = available_years[0]
    combined_daily_data = pd.DataFrame()
    sales_filtered = pd.DataFrame()
    wp_sales_filtered = pd.DataFrame()
    social_filtered = pd.DataFrame()

# ====================== Content =====================
tab_main, tab_sales, tab_social, tab_email, tab_payment_count = st.tabs([
    "📈 Analytics", "Sales Data", "Social Data", "Email Marketing", "Sales by User"
])

with tab_main:
    st.header(f"Daily Sales vs Social Performance ({selected_year})")
    
    if not combined_daily_data.empty:
        # Create base chart with daily x-axis
        base = alt.Chart(combined_daily_data).encode(
            x=alt.X('Date:T', title='Date', axis=alt.Axis(format='%b %d'))
        )
        
        # Sales line - daily
        sales_line = base.transform_filter(
            alt.datum.Type == "Sales"
        ).mark_line(color='blue', strokeWidth=2).encode(
            y=alt.Y('Amount:Q', title='Sales Amount', scale=alt.Scale(zero=False)),
            tooltip=['Date', 'Amount']
        )

        # Live webinar sales line - daily
        wp_sales_line = base.transform_filter(
            alt.datum.Type == "Live"
        ).mark_line(color='green', strokeWidth=2).encode(
            y=alt.Y('Total Amount:Q', scale=alt.Scale(zero=False)),
            tooltip=['Date', 'Total Amount']
        )
        
        # Social scatter - daily
        social_scatter = base.transform_filter(
            alt.datum.Type == "Social"
        ).mark_circle(color='red', size=40).encode(
            y=alt.Y('Total_Score:Q', title='Social Score', scale=alt.Scale(zero=False)),
            tooltip=['Date', 'Total_Score']
        )

        # Combine charts
        combined_chart = alt.layer(sales_line, wp_sales_line, social_scatter).resolve_scale(
            y='shared'
        ).properties(
            width=800,
            height=400
        )
        
        st.altair_chart(combined_chart, use_container_width=True)
        
        # Graph Key
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("🔵 **Blue**: Thinkific")
        with col2:
            st.markdown("🟢 **Green**: Live Webinar") 
        with col3:
            st.markdown("🔴 **Red**: Social Engagement")
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            total_sales = sales_filtered["Amount"].sum() if not sales_filtered.empty else 0
            total_wp_sales = wp_sales_filtered["Total Amount"].sum() if not wp_sales_filtered.empty else 0
            total_all_sales = total_sales + total_wp_sales
            st.metric("Total Sales", f"£{total_all_sales:,.2f}")
        
        with col2:
            st.metric("Total Live Webinar Sales", f"£{total_wp_sales:,.0f}")
        with col3:
            st.metric("Total Thinkific Sales", f"£{total_sales:,.0f}")

        st.divider()
        st.header("Analytics")
        
        # Performance Metrics
        st.subheader("Key Performance Indicators")
        metrics = create_performance_metrics(sales_df, wp_sales_df, social_df)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Revenue", f"£{metrics.get('total_revenue', 0):,.2f}")
        with col2:
            st.metric("Thinkific Revenue", f"£{metrics.get('thinkific_revenue', 0):,.2f}")
        with col3:
            st.metric("Webinar Revenue", f"£{metrics.get('webinar_revenue', 0):,.2f}")
        with col4:
            st.metric("Total Customers", f"{metrics.get('total_customers', 0)}")

        # Best Posting Time Recommendations
        st.subheader("🎯 Best Posting Times & Strategy Recommendations")
        recommendations = analyze_best_posting_times(social_df, sales_df, wp_sales_df)
        
        for recommendation in recommendations:
            st.write(recommendation)
        
        # Monthly Performance Analysis
        st.subheader("📅 Monthly Performance Insights")
                
        if not monthly_sales.empty and not monthly_social.empty:
            # Find peak months for current year
            current_year_sales = monthly_sales[monthly_sales["Year"] == selected_year]
            current_year_social = monthly_social[monthly_social["Year"] == selected_year]
            
            if not current_year_sales.empty and not current_year_social.empty:
                current_peak_sales = current_year_sales.loc[current_year_sales["Amount"].idxmax()]
                current_peak_social = current_year_social.loc[current_year_social["Total_Score"].idxmax()]
            
            # Find peak months overall (all years)
            overall_peak_sales = monthly_sales.loc[monthly_sales["Amount"].idxmax()]
            overall_peak_social = monthly_social.loc[monthly_social["Total_Score"].idxmax()]
            
            # Current Year Peaks
            st.markdown("**Current Year Peaks**")
            col1, col2 = st.columns(2)
            with col1:
                if not current_year_sales.empty:
                    st.info(f"**Peak Sales Month ({selected_year})**: {current_peak_sales['Month_Name']} - £{current_peak_sales['Amount']:,.0f}")
                else:
                    st.info(f"**Peak Sales Month ({selected_year})**: No data")
            with col2:
                if not current_year_social.empty:
                    st.info(f"**Peak Social Month ({selected_year})**: {current_peak_social['Month_Name']} - Score: {current_peak_social['Total_Score']:,.0f}")
                else:
                    st.info(f"**Peak Social Month ({selected_year})**: No data")
            
            # Overall Peaks (All Time)
            st.markdown("**All-Time Peaks**")
            col1, col2 = st.columns(2)
            with col1:
                st.success(f"**All-Time Peak Sales**: {overall_peak_sales['Month_Name']} {int(overall_peak_sales['Year'])} - £{overall_peak_sales['Amount']:,.0f}")
            with col2:
                st.success(f"**All-Time Peak Social**: {overall_peak_social['Month_Name']} {int(overall_peak_social['Year'])} - Score: {overall_peak_social['Total_Score']:,.0f}")
    
        # Data Quality Check
        st.subheader("Numbers")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Social Posts", len(social_df), delta=None)
        with col2:
            st.metric("Thinkific Sales", len(sales_df), delta=None)
        with col3:
            st.metric("Webinar Sales", len(wp_sales_df), delta=None)
        

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