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

def parse_email_date(date_str):
    """Parse email date strings with various formats"""
    try:
        if pd.isna(date_str) or date_str == "":
            return None
        
        # Handle formats like "Tue 07:46" by adding current year and month
        if isinstance(date_str, str) and len(date_str.split()) == 2 and ':' in date_str:
            # Format like "Tue 07:46" - add current date context
            day_abbr, time = date_str.split()
            today = datetime.now()
            
            # Map day abbreviation to actual date (find most recent occurrence)
            days_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
            if day_abbr in days_map:
                current_day = today.weekday()
                target_day = days_map[day_abbr]
                
                # Calculate days difference
                days_diff = (current_day - target_day) % 7
                if days_diff == 0:  # Same day
                    email_date = today
                else:
                    email_date = today - timedelta(days=days_diff)
                
                # Add time
                hour, minute = map(int, time.split(':'))
                email_date = email_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return email_date
        
        # Handle standard date formats like "22/10/2025 13:50"
        return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
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

def prepare_daily_email_data(df):
    """Process email data and aggregate by day"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse dates
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_email_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    
    # Extract year, month, day
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    df_clean["Day"] = df_clean["Parsed_Date"].dt.day
    
    # Count emails per day (you can modify this to track specific types of emails)
    df_clean["Email_Count"] = 1
    
    # Aggregate by day
    daily_email = df_clean.groupby(["Year", "Month", "Day"]).agg({
        "Email_Count": "sum"
    }).reset_index()
    
    # Create proper date column for plotting
    daily_email["Date"] = pd.to_datetime(
        daily_email[["Year", "Month", "Day"]]
    )
    return daily_email

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

def prepare_email_data(df):
    """Process email data and aggregate by month"""
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse dates
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_email_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    
    # Extract year and month
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    
    # Count emails per month
    df_clean["Email_Count"] = 1
    
    # Aggregate by month
    monthly_email = df_clean.groupby(["Year", "Month"]).agg({
        "Email_Count": "sum"
    }).reset_index()
    
    # Create proper date column for plotting (first day of each month)
    monthly_email["Date"] = pd.to_datetime(
        monthly_email[["Year", "Month"]].assign(DAY=1)
    )
    monthly_email["Month_Name"] = monthly_email["Month"].apply(
        lambda x: calendar.month_abbr[x]
    )
    return monthly_email

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
            recommendations.append(f"Best social media month: {best_month_name} (highest engagement)")
    except:
        recommendations.append("Social media data analysis unavailable")
    
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
            recommendations.append(f"Best sales month: {best_sales_month_name} (highest revenue)")
    except:
        recommendations.append("Sales pattern analysis unavailable")
    
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
                recommendations.append("Strong positive correlation between social engagement and sales")
            elif correlation > 0.2:
                recommendations.append("Moderate correlation between social engagement and sales")
            else:
                recommendations.append("Weak correlation between social engagement and sales")
    except:
        recommendations.append("Correlation analysis unavailable")
    

    
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

st.set_page_config(
    page_title="Linguistpd Dashboard",
    page_icon="favicon.jpg",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

# Login system
#    if not check_password():
#        st.stop()

# ====================== Daily Graph setup =====================

try:
    # Prepare daily data for the chart
    daily_sales = prepare_daily_sales_data(sales_df)
    daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
    daily_social = prepare_daily_social_data(social_df)
    daily_email = prepare_daily_email_data(email_df)

    # Prepare monthly data for analysis (keeping existing functionality)
    monthly_sales = prepare_sales_data(sales_df)
    monthly_wp_sales = prepare_wp_sales_data(wp_sales_df)
    monthly_social = prepare_social_data(social_df)
    monthly_email = prepare_email_data(email_df)

    # Get available years from all the dfs
    available_years = sorted(
        set(daily_sales["Year"].unique()).union(
            set(daily_social["Year"].unique()),
            set(daily_wp_sales["Year"].unique()),
            set(daily_email["Year"].unique())
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
    daily_email_filtered = daily_email[daily_email["Year"] == selected_year]

    # Filter monthly data by selected year (for analysis)
    sales_filtered = monthly_sales[monthly_sales["Year"] == selected_year]
    wp_sales_filtered = monthly_wp_sales[monthly_wp_sales["Year"] == selected_year]
    social_filtered = monthly_social[monthly_social["Year"] == selected_year]
    email_filtered = monthly_email[monthly_email["Year"] == selected_year]

    # Combine daily data for shared x-axis
    combined_daily_data = pd.concat([
        daily_sales_filtered.assign(Type="Sales"),
        daily_wp_sales_filtered.assign(Type="Live"),
        daily_social_filtered.assign(Type="Social"),
        daily_email_filtered.assign(Type="Email")
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
    email_filtered = pd.DataFrame()

# ====================== Content =====================
tab_main, tab_sales, tab_social, tab_email, tab_payment_count = st.tabs([
    "📈 Analytics", "Sales Data", "Social Data", "Email Marketing", "Sales by User"
])


# =========== Enhanced Social Media Scoring ==========
def calculate_social_scores(df):
    """Calculate weighted scores for social media posts based on platform performance"""
    if df.empty:
        return pd.DataFrame()
    
    df_clean = df.copy()
    
    # Platform-specific weights based on typical engagement patterns
    platform_weights = {
        'facebook': {'impressions': 0.3, 'comments': 0.4, 'likes': 0.3},
        'twitter': {'impressions': 0.4, 'comments': 0.3, 'likes': 0.3},
        'linkedin': {'impressions': 0.5, 'comments': 0.4, 'likes': 0.1},
        'instagram': {'impressions': 0.3, 'comments': 0.4, 'likes': 0.3}
    }
    
    # Clean engagement data
    engagement_columns = ["Likes/Reactions", "Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    
    for col in engagement_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].apply(
                lambda x: 0 if "no data available" in x.lower() or x.lower() == "nan" or x == "" else x
            )
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    
    # Ensure Platform column exists and clean it
    if 'Platform' not in df_clean.columns:
        df_clean['Platform'] = 'unknown'
    else:
        df_clean['Platform'] = df_clean['Platform'].fillna('unknown').str.lower()
    
    # Calculate platform-specific scores
    df_clean['Platform_Score'] = 0
    
    for platform, weights in platform_weights.items():
        platform_mask = df_clean['Platform'].str.lower() == platform
        if platform_mask.any():
            score = 0
            for metric, weight in weights.items():
                if metric == 'likes' and 'Likes/Reactions' in df_clean.columns:
                    score += df_clean.loc[platform_mask, 'Likes/Reactions'] * weight
                elif metric == 'comments' and 'Comments' in df_clean.columns:
                    score += df_clean.loc[platform_mask, 'Comments'] * weight
                elif metric == 'impressions' and 'Impressions' in df_clean.columns:
                    score += df_clean.loc[platform_mask, 'Impressions'] * weight / 1000  # Normalize impressions
            df_clean.loc[platform_mask, 'Platform_Score'] = score
    
    # Add bonus for multiple engagement types
    engagement_cols = [col for col in ['Likes/Reactions', 'Comments', 'Impressions'] if col in df_clean.columns]
    if engagement_cols:
        engagement_count = (df_clean[engagement_cols] > 0).sum(axis=1)
        df_clean['Engagement_Bonus'] = engagement_count * 0.5
    else:
        df_clean['Engagement_Bonus'] = 0
    
    # Final total score - ensure numeric type
    df_clean['Total_Score'] = pd.to_numeric(df_clean['Platform_Score'] + df_clean['Engagement_Bonus'], errors='coerce')
    
    return df_clean

def analyze_cross_platform_performance(social_df):
    """Analyze which platforms perform best and when"""
    if social_df.empty:
        return pd.DataFrame()
    
    df_clean = calculate_social_scores(social_df)
    
    if df_clean.empty:
        return pd.DataFrame()
    
    # Platform performance analysis
    platform_performance = df_clean.groupby('Platform').agg({
        'Total_Score': ['mean', 'max', 'count'],
        'Likes/Reactions': 'mean',
        'Comments': 'mean',
        'Impressions': 'mean'
    }).round(2)
    
    # Flatten column names
    platform_performance.columns = ['_'.join(col).strip() for col in platform_performance.columns.values]
    platform_performance = platform_performance.reset_index()
    
    # Rename columns for clarity
    platform_performance.columns = ['Platform', 'Avg_Score', 'Max_Score', 'Post_Count', 'Avg_Likes', 'Avg_Comments', 'Avg_Impressions']
    
    return platform_performance

def analyze_email_sales_correlation(daily_email, daily_sales, daily_wp_sales):
    """Analyze correlation between email activity and sales"""
    recommendations = []
    
    try:
        if daily_email.empty or (daily_sales.empty and daily_wp_sales.empty):
            recommendations.append("Need more data for email-sales correlation analysis")
            return recommendations
        
        # Combine sales data
        combined_sales = daily_sales.merge(
            daily_wp_sales[['Date', 'Total Amount']], 
            on='Date', 
            how='outer'
        )
        combined_sales['Total_Sales'] = combined_sales['Amount'].fillna(0) + combined_sales['Total Amount'].fillna(0)
        
        # Merge email and sales data
        merged_data = daily_email.merge(
            combined_sales[['Date', 'Total_Sales']], 
            on='Date', 
            how='inner'
        )
        
        if len(merged_data) > 1:
            # Calculate correlation
            correlation = merged_data['Email_Count'].corr(merged_data['Total_Sales'])
            
            if not pd.isna(correlation):
                if correlation > 0.5:
                    recommendations.append(f"Strong positive correlation between email activity and sales (r={correlation:.2f})")
                elif correlation > 0.2:
                    recommendations.append(f"Moderate positive correlation between email activity and sales (r={correlation:.2f})")
                elif correlation > -0.2:
                    recommendations.append(f"Weak correlation between email activity and sales (r={correlation:.2f})")
                else:
                    recommendations.append(f"Negative correlation between email activity and sales (r={correlation:.2f})")
                
                # Analyze email impact on sales days
                email_days = merged_data[merged_data['Email_Count'] > 0]
                no_email_days = merged_data[merged_data['Email_Count'] == 0]
                
                if len(email_days) > 0 and len(no_email_days) > 0:
                    avg_sales_with_email = email_days['Total_Sales'].mean()
                    avg_sales_without_email = no_email_days['Total_Sales'].mean()
                    
                    if avg_sales_with_email > avg_sales_without_email:
                        boost_percentage = ((avg_sales_with_email - avg_sales_without_email) / avg_sales_without_email * 100) if avg_sales_without_email > 0 else float('inf')
                        recommendations.append(f"Sales on email days are {boost_percentage:.1f}% higher than non-email days")
                    else:
                        recommendations.append("No significant sales boost observed on email days")
                
                # Look for lag effects (emails today affecting sales tomorrow)
                if len(merged_data) > 2:
                    merged_data_sorted = merged_data.sort_values('Date')
                    email_today = merged_data_sorted['Email_Count'].iloc[:-1]
                    sales_tomorrow = merged_data_sorted['Total_Sales'].iloc[1:]
                    
                    if len(email_today) == len(sales_tomorrow):
                        lag_correlation = email_today.corr(sales_tomorrow)
                        if not pd.isna(lag_correlation) and abs(lag_correlation) > 0.3:
                            if lag_correlation > 0:
                                recommendations.append(f"Emails today show positive correlation with sales tomorrow (r={lag_correlation:.2f})")
                            else:
                                recommendations.append(f"Emails today show negative correlation with sales tomorrow (r={lag_correlation:.2f})")
        
        else:
            recommendations.append("Insufficient overlapping data for email-sales correlation analysis")
            
    except Exception as e:
        recommendations.append(f"Email-sales correlation analysis limited: {str(e)}")
    
    return recommendations

def analyze_temporal_patterns(social_df, sales_df, wp_sales_df, email_df):
    """Analyze time-based patterns for recommendations"""
    recommendations = []
    
    try:
        # Prepare daily data for correlation analysis
        daily_social = prepare_daily_social_data(social_df)
        daily_sales = prepare_daily_sales_data(sales_df)
        daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
        daily_email = prepare_daily_email_data(email_df)
        
        if daily_social.empty or (daily_sales.empty and daily_wp_sales.empty):
            recommendations.append("Need more data for temporal pattern analysis")
            return recommendations
        
        # Merge data for lag analysis
        merged_data = daily_social.merge(
            daily_sales[['Date', 'Amount']], 
            on='Date', 
            how='left', 
            suffixes=('_social', '_sales')
        ).merge(
            daily_wp_sales[['Date', 'Total Amount']], 
            on='Date', 
            how='left'
        )
        
        merged_data['Total_Sales'] = merged_data['Amount'].fillna(0) + merged_data['Total Amount'].fillna(0)
        
        # Calculate correlations with different lags
        max_lag = 7  # Look up to 7 days ahead
        best_lag = 0
        best_correlation = 0
        
        for lag in range(max_lag + 1):
            if len(merged_data) > lag + 1:
                # Shift social data forward to see effect on future sales
                social_shifted = merged_data['Total_Score'].iloc[:-lag] if lag > 0 else merged_data['Total_Score']
                sales_future = merged_data['Total_Sales'].iloc[lag:] if lag > 0 else merged_data['Total_Sales']
                
                if len(social_shifted) == len(sales_future) and len(social_shifted) > 1:
                    correlation = social_shifted.corr(sales_future)
                    if not pd.isna(correlation) and abs(correlation) > abs(best_correlation):
                        best_correlation = correlation
                        best_lag = lag
        
        if abs(best_correlation) > 0.3:
            if best_lag == 0:
                recommendations.append(f"Social media shows immediate correlation with sales (r={best_correlation:.2f})")
            else:
                recommendations.append(f"Social media today correlates with sales {best_lag} day(s) later (r={best_correlation:.2f})")
        else:
            recommendations.append("No strong immediate correlation found between social posts and sales")
        
        # Best performing days of week - fix the dtype issue
        df_clean = calculate_social_scores(social_df)
        df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
        df_clean = df_clean.dropna(subset=["Parsed_Date"])
        df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
        df_clean["DayOfWeek"] = df_clean["Parsed_Date"].dt.day_name()
        
        # Ensure Total_Score is numeric before grouping
        df_clean['Total_Score'] = pd.to_numeric(df_clean['Total_Score'], errors='coerce')
        weekday_performance = df_clean.groupby("DayOfWeek")["Total_Score"].mean().sort_values(ascending=False)
        
        if not weekday_performance.empty:
            best_day = weekday_performance.index[0]
            recommendations.append(f"Best posting day: {best_day} has highest average engagement")
        
        # Platform-specific recommendations
        platform_perf = analyze_cross_platform_performance(social_df)
        if not platform_perf.empty:
            best_platform = platform_perf.loc[platform_perf['Avg_Score'].idxmax(), 'Platform']
            recommendations.append(f"Top platform: {best_platform.capitalize()} delivers highest average engagement")
            
            # Check if any platform is underutilized but effective
            efficient_platforms = platform_perf[platform_perf['Post_Count'] < platform_perf['Post_Count'].median()]
            if not efficient_platforms.empty:
                efficient_platform = efficient_platforms.loc[efficient_platforms['Avg_Score'].idxmax(), 'Platform']
                recommendations.append(f"Opportunity: Consider posting more on {efficient_platform.capitalize()} - it shows good engagement with fewer posts")
        
        # Content gap analysis (based on your sample data)
        corporate_posts = social_df[social_df['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
        if len(corporate_posts) > 0:
            avg_corporate_score = calculate_social_scores(corporate_posts)['Total_Score'].mean()
            other_posts = social_df[~social_df['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
            if len(other_posts) > 0:
                avg_other_score = calculate_social_scores(other_posts)['Total_Score'].mean()
                if avg_corporate_score > avg_other_score * 1.2:
                    recommendations.append("Corporate training posts perform 20%+ better than average - consider expanding this content")
        
    except Exception as e:
        recommendations.append(f"Pattern analysis limited: {str(e)}")
    
    return recommendations

def analyze_seasonal_trends(monthly_social, monthly_sales, monthly_wp_sales):
    """Analyze seasonal patterns across all data"""
    recommendations = []
    
    try:
        # Combine all monthly data
        merged_monthly = monthly_social.merge(
            monthly_sales[['Year', 'Month', 'Amount']], 
            on=['Year', 'Month'], 
            how='outer', 
            suffixes=('_social', '_sales')
        ).merge(
            monthly_wp_sales[['Year', 'Month', 'Total Amount']], 
            on=['Year', 'Month'], 
            how='outer'
        )
        
        merged_monthly['Total_Sales'] = merged_monthly['Amount'].fillna(0) + merged_monthly['Total Amount'].fillna(0)
        merged_monthly['Total_Score'] = merged_monthly['Total_Score'].fillna(0)
        
        # Analyze by month (across all years)
        monthly_patterns = merged_monthly.groupby('Month').agg({
            'Total_Sales': 'mean',
            'Total_Score': 'mean'
        }).reset_index()
        
        if not monthly_patterns.empty:
            best_sales_month = monthly_patterns.loc[monthly_patterns['Total_Sales'].idxmax(), 'Month']
            best_social_month = monthly_patterns.loc[monthly_patterns['Total_Score'].idxmax(), 'Month']
            best_sales_month_name = calendar.month_name[best_sales_month]
            best_social_month_name = calendar.month_name[best_social_month]
            
            recommendations.append(f"Seasonal peak sales: {best_sales_month_name} historically strongest for revenue")
            recommendations.append(f"Seasonal peak engagement: {best_social_month_name} historically best for social engagement")
            
            # Check alignment
            if best_sales_month == best_social_month:
                recommendations.append(f"Perfect alignment: {best_sales_month_name} is peak for both sales AND engagement - maximize efforts this month!")
            else:
                recommendations.append(f"Strategic planning: Consider increasing social activity in {best_social_month_name} to build momentum for {best_sales_month_name} sales peak")
        
    except Exception as e:
        recommendations.append("Seasonal analysis limited by available data")
    
    return recommendations

def create_performance_metrics(sales_df, wp_sales_df, social_df, email_df):
    """Create key performance metrics with proper error handling"""
    metrics = {}
    
    try:
        # Total revenue - ensure numeric values
        if "Amount" in sales_df.columns:
            sales_df["Amount"] = pd.to_numeric(sales_df["Amount"], errors='coerce').fillna(0)
            thinkific_sales = sales_df["Amount"].sum()
        else:
            thinkific_sales = 0
            
        if "Total Amount" in wp_sales_df.columns:
            wp_sales_df["Total Amount"] = pd.to_numeric(wp_sales_df["Total Amount"], errors='coerce').fillna(0)
            webinar_sales = wp_sales_df["Total Amount"].sum()
        else:
            webinar_sales = 0
            
        metrics["total_revenue"] = thinkific_sales + webinar_sales
        metrics["thinkific_revenue"] = thinkific_sales
        metrics["webinar_revenue"] = webinar_sales
        
        # Social engagement with enhanced scoring
        social_scored = calculate_social_scores(social_df)
        if not social_scored.empty and "Total_Score" in social_scored.columns:
            metrics["avg_engagement"] = social_scored["Total_Score"].mean()
            metrics["max_engagement"] = social_scored["Total_Score"].max()
            metrics["total_posts"] = len(social_scored)
            
            # Platform diversity
            if 'Platform' in social_scored.columns:
                platforms = social_scored['Platform'].nunique()
                metrics["platform_diversity"] = platforms
        
        # Email metrics
        if not email_df.empty:
            metrics["total_emails"] = len(email_df)
            # Count training emails specifically
            training_emails = email_df[email_df['Sender'].str.contains('training', case=False, na=False)]
            metrics["training_emails"] = len(training_emails)
        
        # Growth metrics - ensure we're counting valid rows
        valid_sales = len(sales_df[sales_df["Amount"].notna()]) if "Amount" in sales_df.columns else 0
        valid_wp_sales = len(wp_sales_df[wp_sales_df["Total Amount"].notna()]) if "Total Amount" in wp_sales_df.columns else 0
        metrics["total_customers"] = valid_sales + valid_wp_sales
        
    except Exception as e:
        st.error(f"Error calculating metrics: {e}")
        # Set default values
        metrics = {
            "total_revenue": 0,
            "thinkific_revenue": 0,
            "webinar_revenue": 0,
            "total_customers": 0,
            "avg_engagement": 0,
            "max_engagement": 0,
            "total_posts": 0,
            "platform_diversity": 0,
            "total_emails": 0,
            "training_emails": 0
        }
    
    return metrics

# =========== Enhanced Analytics Tab Section ==========
with tab_main:
    st.header(f"Daily Sales vs Social vs Email Performance ({selected_year})")
    
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

        # Email scatter - daily (added email data point)
        email_scatter = base.transform_filter(
            alt.datum.Type == "Email"
        ).mark_circle(color='orange', size=40).encode(
            y=alt.Y('Email_Count:Q', title='Email Count', scale=alt.Scale(zero=False)),
            tooltip=['Date', 'Email_Count']
        )

        # Combine charts
        combined_chart = alt.layer(sales_line, wp_sales_line, social_scatter, email_scatter).resolve_scale(
            y='shared'
        ).properties(
            width=800,
            height=400
        )
        
        st.altair_chart(combined_chart, use_container_width=True)
        
        # Graph Key
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown("🔵 **Blue**: Thinkific")
        with col2:
            st.markdown("🟢 **Green**: Live Webinar") 
        with col3:
            st.markdown("🔴 **Red**: Social Engagement")
        with col4:
            st.markdown("🟠 **Orange**: Email Count")
        
        # Metrics
        col, col1,col2, col3 = st.columns(4)
        with col:
            total_sales = sales_filtered["Amount"].sum() if not sales_filtered.empty else 0
            total_wp_sales = wp_sales_filtered["Total Amount"].sum() if not wp_sales_filtered.empty else 0
            total_all_sales = total_sales + total_wp_sales
            st.metric("Total Yearly Revenue", total_all_sales.round(1))

        with col1:
            st.metric("Total Yearly Sales", len(wp_sales_df+sales_df))
        with col2:
            st.metric("Total Yearly Webinar Sales", f"£{total_wp_sales:,.0f}")
        with col3:
            st.metric("Total Yearly Thinkific Sales", f"£{total_sales:,.0f}")

        st.divider()
        st.header("Enhanced Analytics")
        
        # Performance Metrics
        st.subheader("Key Performance Indicators")
        metrics = create_performance_metrics(sales_df, wp_sales_df, social_df, email_df)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Revenue", f"£{metrics.get('total_revenue', 0).round(1)}")
        with col2:
            st.metric("Thinkific Revenue", f"£{metrics.get('thinkific_revenue', 0).round(1)}")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Webinar Revenue", f"£{metrics.get('webinar_revenue', 0).round(1)}")
        with col2:
            st.metric("Total Customers", f"{metrics.get('total_customers', 0)}")

        # Enhanced Analytics Section
        st.subheader("Data-Driven Recommendations")
        
        # Email-Sales Correlation Analysis
        st.subheader("Email Marketing Impact")
        email_recommendations = analyze_email_sales_correlation(daily_email, daily_sales, daily_wp_sales)
        for recommendation in email_recommendations:
            st.write(recommendation)
        
        # Cross-platform analysis
        st.subheader("Platform Performance Analysis")
        platform_perf = analyze_cross_platform_performance(social_df)
        if not platform_perf.empty:
            # Create metrics for top platforms
            top_platform = platform_perf.loc[platform_perf['Avg_Score'].idxmax()]
            most_active_platform = platform_perf.loc[platform_perf['Post_Count'].idxmax()]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Highest Performing Platform", 
                         f"{top_platform['Platform'].capitalize()}",
                         f"Score: {top_platform['Avg_Score']:.1f}")
            with col2:
                st.metric("Most Active Platform",
                         f"{most_active_platform['Platform'].capitalize()}",
                         f"{most_active_platform['Post_Count']} posts")
            with col3:
                underutilized = platform_perf[platform_perf['Post_Count'] < platform_perf['Post_Count'].median()]
                if not underutilized.empty:
                    opportunity_platform = underutilized.loc[underutilized['Avg_Score'].idxmax()]
                    st.metric("Opportunity Platform",
                             f"{opportunity_platform['Platform'].capitalize()}",
                             f"{opportunity_platform['Post_Count']} posts")
            
            st.dataframe(platform_perf, use_container_width=True)
        else:
            st.info("No platform performance data available")
        
        # Temporal patterns
        st.subheader("Engagement and Sales Patterns")
        temporal_recommendations = analyze_temporal_patterns(social_df, sales_df, wp_sales_df, email_df)
        for recommendation in temporal_recommendations:
            st.write(recommendation)
        
        # Seasonal trends
        st.subheader("Seasonal Trends")
        seasonal_recommendations = analyze_seasonal_trends(monthly_social, monthly_sales, monthly_wp_sales)
        for recommendation in seasonal_recommendations:
            st.write(recommendation)

        # Data Summary
        st.subheader("Data Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Social Posts", len(social_df))
        with col2:
            st.metric("Thinkific Sales", len(sales_df))
        with col3:
            st.metric("Webinar Sales", len(wp_sales_df))
        with col4:
            st.metric("Emails", len(email_df))

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

    # Thinkific Sales - using "Email address" column
    if "Email address" in sales_df.columns:
        group = sales_df.groupby("Email address")
        total_amount = group["Amount"].sum().reset_index(name="Amount")
        purchase_counts = sales_df["Email address"].value_counts().reset_index(name="Purchase Count")
        purchase_counts["Amount Spent"] = total_amount["Amount"]
        purchase_counts.columns = ["Email address", "Purchase Count", "Amount Spent"]
        
        st.subheader("Thinkific Sales")
        st.write(purchase_counts)
    else:
        st.warning("Thinkific sales data: 'Email address' column not found")
        purchase_counts = pd.DataFrame()

    # WordPress Sales - using "Email" column  
    if "Email" in wp_sales_df.columns:
        wp_group = wp_sales_df.groupby("Email")
        total_wp_amount = wp_group["Total Amount"].sum().reset_index(name="Total Amount")
        purchase_counts_wp = wp_sales_df["Email"].value_counts().reset_index()
        purchase_counts_wp["Amount Spent"] = total_wp_amount["Total Amount"]
        purchase_counts_wp.columns = ["Email", "Purchase Count", "Amount Spent"]
        
        st.subheader("Live Webinar Sales")
        st.write(purchase_counts_wp)
    else:
        st.warning("WordPress sales data: 'Email' column not found")
        purchase_counts_wp = pd.DataFrame()

    # Combine counts if both datasets are available
    if not purchase_counts.empty and not purchase_counts_wp.empty:
        st.subheader("Combined Data")
        # Rename columns to be consistent before combining
        purchase_counts_renamed = purchase_counts.rename(columns={"Email address": "Email"})
        purchase_counts_wp_renamed = purchase_counts_wp.rename(columns={"Email": "Email"})
        
        combine_counts = pd.concat([purchase_counts_renamed, purchase_counts_wp_renamed], ignore_index=True)
        combined_group = combine_counts.groupby("Email")
        total_group = combined_group["Purchase Count"].sum().reset_index(name="Purchase Count")
        total_group_amount = combined_group["Amount Spent"].sum().reset_index(name="Amount Spent")
        
        # Create final combined dataframe
        final_combined = pd.DataFrame({
            "Email": total_group["Email"],
            "Purchase Count": total_group["Purchase Count"],
            "Amount Spent": total_group_amount["Amount Spent"]
        })
        
        st.write(final_combined)
    elif not purchase_counts.empty or not purchase_counts_wp.empty:
        st.info("Only one dataset available for combination")
    else:
        st.warning("No sales data available for analysis")

st.divider()


#=====running the external scraper scrits======


st.header("Data Loaders")
st.write("Run to get the latest data - they might take a while it they havent been ran in a while :/")



wp_loader, email_loader, social_loader = st.columns(3)

#somehow functional pulling cerds from the tmol
with wp_loader:
    if st.button("WordPress data loader"):

        wp_user = st.secrets.wp_credentials.WP_USERNAME
        wp_pass = st.secrets.wp_credentials.WP_PASSWORD
        sa = st.secrets.gcp_service_account

        env = os.environ.copy()
        env["WP_USERNAME"] = wp_user
        env["WP_PASSWORD"] = wp_pass


        # mapping the service account info
        env["SHEET_TYPE"] = sa.type
        env["SHEET_PROJECT_ID"] = sa.project_id
        env["SHEET_PRIVATE_KEY_ID"] = sa.private_key_id
        env["SHEET_PRIVATE_KEY"] = sa.private_key
        env["SHEET_CLIENT_EMAIL"] = sa.client_email
        env["SHEET_CLIENT_ID"] = sa.client_id
        env["SHEET_AUTH_URI"] = sa.auth_uri
        env["SHEET_TOKEN_URI"] = sa.token_uri
        env["SHEET_AUTH_PROVIDER_X509_CERT_URL"] = sa.auth_provider_x509_cert_url
        env["SHEET_CLIENT_X509_CERT_URL"] = sa.client_x509_cert_url


        result = subprocess.run(
            [sys.executable, "lpd-data-scrapers/wp_scraper.py"],
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            st.error(f"Error: {result.stderr}")
        else:
            st.success(result.stdout)


with email_loader:
    if st.button('email data loader'):
        email_user = st.secrets.email_credentials.MAIL_ADR
        email_pass = st.secrets.email_credentials.PASSWD
        sa = st.secrets.gcp_service_account


        env = os.environ.copy()
        env["MAIL_ADR"] = email_user
        env["PASSWD"] = email_pass
        

        # mapping the service account info
        env["SHEET_TYPE"] = sa.type
        env["SHEET_PROJECT_ID"] = sa.project_id
        env["SHEET_PRIVATE_KEY_ID"] = sa.private_key_id
        env["SHEET_PRIVATE_KEY"] = sa.private_key
        env["SHEET_CLIENT_EMAIL"] = sa.client_email
        env["SHEET_CLIENT_ID"] = sa.client_id
        env["SHEET_AUTH_URI"] = sa.auth_uri
        env["SHEET_TOKEN_URI"] = sa.token_uri
        env["SHEET_AUTH_PROVIDER_X509_CERT_URL"] = sa.auth_provider_x509_cert_url
        env["SHEET_CLIENT_X509_CERT_URL"] = sa.client_x509_cert_url

        result = subprocess.run(
            [sys.executable, "lpd-data-scrapers/email_data.py"],
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            st.error(f"Error: {result.stderr}")
        else:
            st.success(result.stdout)

with social_loader:
    if st.button('social data loader'):
        buffer_user = st.secrets.buffer_credentials.buffer_user
        buffer_pass = st.secrets.buffer_credentials.buffer_pass
        sa = st.secrets.gcp_service_account

        env = os.environ.copy()
        env["buffer_user"] = buffer_user
        env["buffer_pass"] = buffer_pass

        # mapping the service account info
        env["SHEET_TYPE"] = sa.type
        env["SHEET_PROJECT_ID"] = sa.project_id
        env["SHEET_PRIVATE_KEY_ID"] = sa.private_key_id
        env["SHEET_PRIVATE_KEY"] = sa.private_key
        env["SHEET_CLIENT_EMAIL"] = sa.client_email
        env["SHEET_CLIENT_ID"] = sa.client_id
        env["SHEET_AUTH_URI"] = sa.auth_uri
        env["SHEET_TOKEN_URI"] = sa.token_uri
        env["SHEET_AUTH_PROVIDER_X509_CERT_URL"] = sa.auth_provider_x509_cert_url
        env["SHEET_CLIENT_X509_CERT_URL"] = sa.client_x509_cert_url


        result = subprocess.run(
            [sys.executable, "lpd-data-scrapers/buffer.py"],
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            st.error(f"Error: {result.stderr}")
        else:
            st.success(result.stdout)