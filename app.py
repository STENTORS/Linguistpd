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
import re

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
    """Process social data and aggregate by day - count posts per day"""
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
    
    # Count posts per day instead of using engagement scores
    df_clean["Post_Count"] = 1
    
    # Aggregate by day - count posts
    daily_social = df_clean.groupby(["Year", "Month", "Day"]).agg({
        "Post_Count": "sum"
    }).reset_index()
    
    # Rename for consistency with chart
    daily_social["Total_Score"] = daily_social["Post_Count"]
    
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
)

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

def analyze_posting_times_vs_sales(social_df, sales_df, wp_sales_df):
    """Analyze correlation between social media posting times and sales"""
    results = {}
    
    try:
        # Prepare sales data
        daily_sales = prepare_daily_sales_data(sales_df)
        daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
        
        if daily_sales.empty and daily_wp_sales.empty:
            return results
        
        # Combine sales
        combined_sales = daily_sales.merge(
            daily_wp_sales[['Date', 'Total Amount']], 
            on='Date', 
            how='outer'
        )
        combined_sales['Total_Sales'] = combined_sales['Amount'].fillna(0) + combined_sales['Total Amount'].fillna(0)
        
        # Process social media data - extract posting times
        social_clean = social_df.copy()
        social_clean["Parsed_Date"] = social_clean["Date"].apply(parse_social_date)
        social_clean = social_clean.dropna(subset=["Parsed_Date"])
        social_clean["Parsed_Date"] = pd.to_datetime(social_clean["Parsed_Date"])
        social_clean["DayOfWeek"] = social_clean["Parsed_Date"].dt.day_name()
        social_clean["DayOfWeekNum"] = social_clean["Parsed_Date"].dt.dayofweek
        
        # Extract time if available
        if "Time" in social_clean.columns:
            social_clean["Hour"] = social_clean["Time"].apply(
                lambda x: pd.to_datetime(x, format="%H:%M", errors='coerce').hour if pd.notna(x) and isinstance(x, str) and ':' in x else None
            )
        else:
            social_clean["Hour"] = None
        
        # Analyze day of week correlation
        # Count posts per day and merge with sales
        posts_by_day = social_clean.groupby(["Parsed_Date", "DayOfWeek"]).size().reset_index(name="Post_Count")
        posts_by_day["Date"] = pd.to_datetime(posts_by_day["Parsed_Date"]).dt.date
        combined_sales["Date"] = pd.to_datetime(combined_sales["Date"]).dt.date
        
        merged = posts_by_day.merge(
            combined_sales[["Date", "Total_Sales"]],
            on="Date",
            how="inner"
        )
        
        if not merged.empty:
            # Day of week analysis
            day_sales = merged.groupby("DayOfWeek").agg({
                "Total_Sales": "sum",
                "Post_Count": "sum"
            }).reset_index()
            
            if not day_sales.empty:
                best_sales_day = day_sales.loc[day_sales['Total_Sales'].idxmax(), 'DayOfWeek']
                most_posts_day = day_sales.loc[day_sales['Post_Count'].idxmax(), 'DayOfWeek']
                
                results['best_sales_day'] = best_sales_day
                results['most_posts_day'] = most_posts_day
                results['day_sales_data'] = day_sales
        
        # Lag analysis - posts today vs sales in future days
        posts_daily = social_clean.groupby("Parsed_Date").size().reset_index(name="Post_Count")
        posts_daily["Date"] = posts_daily["Parsed_Date"].dt.date
        
        best_lag = 0
        best_correlation = 0
        
        for lag in range(8):  # Check 0-7 days ahead
            if len(posts_daily) > lag:
                posts_shifted = posts_daily["Post_Count"].iloc[:-lag] if lag > 0 else posts_daily["Post_Count"]
                sales_future = combined_sales["Total_Sales"].iloc[lag:] if lag > 0 else combined_sales["Total_Sales"]
                
                if len(posts_shifted) == len(sales_future) and len(posts_shifted) > 1:
                    correlation = posts_shifted.corr(sales_future)
                    if not pd.isna(correlation) and abs(correlation) > abs(best_correlation):
                        best_correlation = correlation
                        best_lag = lag
        
        results['best_lag'] = best_lag
        results['best_correlation'] = best_correlation
        
    except Exception as e:
        print(f"Error in posting times analysis: {e}")
    
    return results

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
        
        # Social media metrics - just count posts and platforms
        if not social_df.empty:
            metrics["total_posts"] = len(social_df)
            
            # Platform diversity
            if 'Platform' in social_df.columns:
                platforms = social_df['Platform'].nunique()
                metrics["platform_diversity"] = platforms
            else:
                metrics["platform_diversity"] = 0
        
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
        
        # Social scatter - daily (showing post count)
        social_scatter = base.transform_filter(
            alt.datum.Type == "Social"
        ).mark_circle(color='red', size=40).encode(
            y=alt.Y('Total_Score:Q', title='Posts Count', scale=alt.Scale(zero=False)),
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
            st.markdown("🔴 **Red**: Social Posts")
        with col4:
            st.markdown("🟠 **Orange**: Email Count")
        


        st.divider()



        
        # Yearly Metrics (based on filter)
        st.subheader(f"Yearly Metrics ({selected_year})")
        col1, col3, col4 = st.columns(3)
        
        # Calculate yearly metrics from filtered data
        yearly_thinkific_sales = sales_filtered["Amount"].sum() if not sales_filtered.empty and "Amount" in sales_filtered.columns else 0
        yearly_wp_sales = wp_sales_filtered["Total Amount"].sum() if not wp_sales_filtered.empty and "Total Amount" in wp_sales_filtered.columns else 0
        yearly_total_revenue = yearly_thinkific_sales + yearly_wp_sales
        
        # Count yearly sales
        yearly_thinkific_count = len(sales_filtered) if not sales_filtered.empty else 0
        yearly_wp_count = len(wp_sales_filtered) if not wp_sales_filtered.empty else 0
        
        with col1:
            st.metric("Revenue", f"£{yearly_total_revenue:,.2f}")
        with col3:
            st.metric("Thinkific Revenue", f"£{yearly_thinkific_sales:,.2f}")
        with col4:
            st.metric("Webinar Revenue", f"£{yearly_wp_sales:,.2f}")

        st.divider()




        metrics = create_performance_metrics(sales_df, wp_sales_df, social_df, email_df)
                


        st.header("Analytics (all data)")

        # Performance Metrics
        st.subheader("Total Metrics")

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










#OTHER TABS

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
        # Ensure Amount is numeric
        sales_df["Amount"] = pd.to_numeric(sales_df["Amount"], errors='coerce').fillna(0)
        
        # Group by email and calculate both count and sum properly
        thinkific_summary = sales_df.groupby("Email address").agg({
            "Amount": ["sum", "count"]
        }).reset_index()
        thinkific_summary.columns = ["Email address", "Amount Spent", "Purchase Count"]
        thinkific_summary = thinkific_summary[["Email address", "Purchase Count", "Amount Spent"]]
        
        st.subheader("Thinkific Sales")
        st.write(thinkific_summary)
    else:
        st.warning("Thinkific sales data: 'Email address' column not found")
        thinkific_summary = pd.DataFrame()

    # WordPress Sales - using "Email" column  
    if "Email" in wp_sales_df.columns:
        # Ensure Total Amount is numeric
        wp_sales_df["Total Amount"] = pd.to_numeric(wp_sales_df["Total Amount"], errors='coerce').fillna(0)
        
        # Group by email and calculate both count and sum properly
        wp_summary = wp_sales_df.groupby("Email").agg({
            "Total Amount": ["sum", "count"]
        }).reset_index()
        wp_summary.columns = ["Email", "Amount Spent", "Purchase Count"]
        wp_summary = wp_summary[["Email", "Purchase Count", "Amount Spent"]]
        
        st.subheader("Live Webinar Sales")
        st.write(wp_summary)
    else:
        st.warning("WordPress sales data: 'Email' column not found")
        wp_summary = pd.DataFrame()

    # Combine counts if both datasets are available
    if not thinkific_summary.empty and not wp_summary.empty:
        st.subheader("Combined Data")
        # Rename columns to be consistent before combining
        thinkific_renamed = thinkific_summary.rename(columns={"Email address": "Email"})
        wp_renamed = wp_summary.rename(columns={"Email": "Email"})
        
        # Combine the dataframes
        combined_df = pd.concat([thinkific_renamed, wp_renamed], ignore_index=True)
        
        # Group by Email and sum both Purchase Count and Amount Spent
        final_combined = combined_df.groupby("Email").agg({
            "Purchase Count": "sum",
            "Amount Spent": "sum"
        }).reset_index()
        
        # Sort by Amount Spent descending for better readability
        final_combined = final_combined.sort_values("Amount Spent", ascending=False).reset_index(drop=True)
        
        st.write(final_combined)
    elif not thinkific_summary.empty or not wp_summary.empty:
        st.info("Only one dataset available for combination")
    else:
        st.warning("No sales data available for analysis")



