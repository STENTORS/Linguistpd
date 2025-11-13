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
    
    # Final total score
    df_clean['Total_Score'] = df_clean['Platform_Score'] + df_clean['Engagement_Bonus']
    
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

def analyze_temporal_patterns(social_df, sales_df, wp_sales_df):
    """Analyze time-based patterns for recommendations"""
    recommendations = []
    
    try:
        # Prepare daily data for correlation analysis
        daily_social = prepare_daily_social_data(social_df)
        daily_sales = prepare_daily_sales_data(sales_df)
        daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
        
        if daily_social.empty or (daily_sales.empty and daily_wp_sales.empty):
            recommendations.append("📊 Need more data for temporal pattern analysis")
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
                recommendations.append(f"🎯 **Immediate Impact**: Social media shows immediate correlation with sales (r={best_correlation:.2f})")
            else:
                recommendations.append(f"🎯 **Delayed Impact**: Social media today correlates with sales {best_lag} day(s) later (r={best_correlation:.2f})")
        else:
            recommendations.append("📊 **Weak Correlation**: No strong immediate correlation found between social posts and sales")
        
        # Best performing days of week
        df_clean = calculate_social_scores(social_df)
        df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
        df_clean = df_clean.dropna(subset=["Parsed_Date"])
        df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
        df_clean["DayOfWeek"] = df_clean["Parsed_Date"].dt.day_name()
        
        weekday_performance = df_clean.groupby("DayOfWeek")["Total_Score"].mean().sort_values(ascending=False)
        if not weekday_performance.empty:
            best_day = weekday_performance.index[0]
            recommendations.append(f"📅 **Best Posting Day**: {best_day} has highest average engagement")
        
        # Platform-specific recommendations
        platform_perf = analyze_cross_platform_performance(social_df)
        if not platform_perf.empty:
            best_platform = platform_perf.loc[platform_perf['Avg_Score'].idxmax(), 'Platform']
            recommendations.append(f"📱 **Top Platform**: {best_platform.capitalize()} delivers highest average engagement")
            
            # Check if any platform is underutilized but effective
            efficient_platforms = platform_perf[platform_perf['Post_Count'] < platform_perf['Post_Count'].median()]
            if not efficient_platforms.empty:
                efficient_platform = efficient_platforms.loc[efficient_platforms['Avg_Score'].idxmax(), 'Platform']
                recommendations.append(f"💡 **Opportunity**: Consider posting more on {efficient_platform.capitalize()} - it shows good engagement with fewer posts")
        
        # Content gap analysis (based on your sample data)
        corporate_posts = social_df[social_df['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
        if len(corporate_posts) > 0:
            avg_corporate_score = calculate_social_scores(corporate_posts)['Total_Score'].mean()
            other_posts = social_df[~social_df['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
            if len(other_posts) > 0:
                avg_other_score = calculate_social_scores(other_posts)['Total_Score'].mean()
                if avg_corporate_score > avg_other_score * 1.2:
                    recommendations.append("🌟 **Content Strength**: Corporate training posts perform 20%+ better than average - consider expanding this content")
        
    except Exception as e:
        recommendations.append(f"📊 Pattern analysis limited: {str(e)}")
    
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
            
            recommendations.append(f"📈 **Seasonal Peak Sales**: {best_sales_month_name} historically strongest for revenue")
            recommendations.append(f"👥 **Seasonal Peak Engagement**: {best_social_month_name} historically best for social engagement")
            
            # Check alignment
            if best_sales_month == best_social_month:
                recommendations.append(f"🎯 **Perfect Alignment**: {best_sales_month_name} is peak for both sales AND engagement - maximize efforts this month!")
            else:
                recommendations.append(f"🔄 **Strategic Planning**: Consider increasing social activity in {best_social_month_name} to build momentum for {best_sales_month_name} sales peak")
        
    except Exception as e:
        recommendations.append("📅 Seasonal analysis limited by available data")
    
    return recommendations

def create_performance_metrics(sales_df, wp_sales_df, social_df):
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
            "platform_diversity": 0
        }
    
    return metrics

# =========== Enhanced Analytics Tab Section ==========
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
        st.header("Enhanced Analytics")
        
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

        # Enhanced Analytics Section
        st.subheader("🎯 Data-Driven Recommendations")
        
        # Cross-platform analysis
        platform_perf = analyze_cross_platform_performance(social_df)
        if not platform_perf.empty:
            st.write("**Platform Performance Analysis:**")
            
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
        st.subheader("📊 Engagement & Sales Patterns")
        temporal_recommendations = analyze_temporal_patterns(social_df, sales_df, wp_sales_df)
        for recommendation in temporal_recommendations:
            st.write(recommendation)
        
        # Seasonal trends
        st.subheader("📅 Seasonal Trends")
        seasonal_recommendations = analyze_seasonal_trends(monthly_social, monthly_sales, monthly_wp_sales)
        for recommendation in seasonal_recommendations:
            st.write(recommendation)

        # Content Performance Analysis
        st.subheader("📝 Content Performance")
        try:
            social_scored = calculate_social_scores(social_df)
            
            if not social_scored.empty:
                # Analyze content themes from your sample data
                content_analysis = []
                
                # Corporate training posts (from your sample)
                corporate_posts = social_scored[social_scored['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
                if len(corporate_posts) > 0:
                    corp_avg_score = corporate_posts['Total_Score'].mean()
                    content_analysis.append({
                        'Content Type': 'Corporate Training',
                        'Posts': len(corporate_posts),
                        'Avg Score': corp_avg_score,
                        'Performance': 'High' if corp_avg_score > social_scored['Total_Score'].mean() * 1.2 else 'Average'
                    })
                
                # Other content analysis can be added based on your actual post content
                other_posts = social_scored[~social_scored['Post'].str.contains('corporate|CPD|NHS|government', case=False, na=False)]
                if len(other_posts) > 0:
                    other_avg_score = other_posts['Total_Score'].mean()
                    content_analysis.append({
                        'Content Type': 'Other Content',
                        'Posts': len(other_posts),
                        'Avg Score': other_avg_score,
                        'Performance': 'High' if other_avg_score > social_scored['Total_Score'].mean() * 1.2 else 'Average'
                    })
                
                if content_analysis:
                    content_df = pd.DataFrame(content_analysis)
                    st.dataframe(content_df, use_container_width=True)
                    
                    # Content recommendations
                    best_content = content_df.loc[content_df['Avg Score'].idxmax()]
                    if best_content['Performance'] == 'High':
                        st.success(f"🌟 **Content Insight**: {best_content['Content Type']} posts perform best with average score of {best_content['Avg Score']:.1f}")
                else:
                    st.info("Add more post content to enable content performance analysis")
                
        except Exception as e:
            st.info("Content analysis requires more post data for deeper insights")

        # Data Summary
        st.subheader("📊 Data Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Social Posts", len(social_df))
        with col2:
            st.metric("Thinkific Sales", len(sales_df))
        with col3:
            st.metric("Webinar Sales", len(wp_sales_df))
        with col4:
            total_engagement = social_scored['Total_Score'].sum() if not social_scored.empty and 'Total_Score' in social_scored.columns else 0
            st.metric("Total Engagement Score", f"{total_engagement:.0f}")

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