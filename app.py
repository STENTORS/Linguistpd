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

# =========== Page Config (MUST be first Streamlit call) ==========
st.set_page_config(
    page_title="Linguistpd Dashboard",
    page_icon="favicon.jpg",
    initial_sidebar_state="expanded",
)

# =========== Sheet Connections ==========
social_conn = st.connection("social_gsheets", type=GSheetsConnection)
social_df = social_conn.read()

sales_conn = st.connection("sales_gsheets", type=GSheetsConnection)
sales_df = sales_conn.read(worksheet="Thinkifc orders updated by Zapier")

wp_sales_conn = st.connection("wp_sales_gsheets", type=GSheetsConnection)
wp_sales_df = wp_sales_conn.read()

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

TODAY = datetime.now().date()

def parse_social_date(date_str):
    """
    Parse Buffer social date strings into datetime.date objects.
    Handles:
      - "Today, 5 March"  / "Today"
      - "Yesterday, 4 March" / "Yesterday"
      - "Tuesday, 12 August"  (strips weekday, parses rest)
      - "5 March"  / "March 5"  (yearless — infers year, never future)
      - "5 March 2024"  (with year)
    Never returns a date beyond today.
    """
    try:
        if pd.isna(date_str) or str(date_str).strip() == "":
            return None

        s = str(date_str).strip()
        lower = s.lower()

        # Relative keywords
        if lower.startswith("today"):
            return TODAY
        if lower.startswith("yesterday"):
            return TODAY - timedelta(days=1)

        # Strip leading weekday: "Tuesday, 12 August" -> "12 August"
        s = re.sub(
            r'^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[,\s]+',
            '', s, flags=re.IGNORECASE
        ).strip()

        # Try parsing with dateutil
        parsed = parser.parse(s, dayfirst=True).date()

        # If the parsed date is in the future, dateutil picked the current year
        # but the post hasn't happened yet — roll back one year
        if parsed > TODAY:
            parsed = parsed.replace(year=parsed.year - 1)

        return parsed
    except Exception:
        return None


def clean_wp_date(date_str):
    """Parse WordPress sales dates."""
    try:
        if pd.isna(date_str) or date_str == "":
            return None
        if isinstance(date_str, str):
            date_str = ' '.join(date_str.split())
            try:
                result = pd.to_datetime(date_str)
                # Reject future dates
                if result.date() > TODAY:
                    return None
                return result
            except Exception:
                pass
            parts = date_str.split()
            if len(parts) >= 2:
                date_part = parts[0] if len(parts[0].split('-')) == 3 else parts[1]
                time_part = parts[-1] if ':' in parts[-1] else '00:00:00'
                result = pd.to_datetime(f"{date_part} {time_part}")
                if result.date() > TODAY:
                    return None
                return result
        result = pd.to_datetime(date_str)
        if result.date() > TODAY:
            return None
        return result
    except Exception:
        return None


def parse_email_date(date_str):
    """Parse email date strings with various formats. Never returns a future date."""
    try:
        if pd.isna(date_str) or date_str == "":
            return None

        # Format like "Tue 07:46" — resolve to most recent occurrence of that weekday
        if isinstance(date_str, str) and len(date_str.split()) == 2 and ':' in date_str:
            day_abbr, time = date_str.split()
            today_dt = datetime.now()
            days_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
            if day_abbr in days_map:
                days_diff = (today_dt.weekday() - days_map[day_abbr]) % 7
                email_date = today_dt if days_diff == 0 else today_dt - timedelta(days=days_diff)
                hour, minute = map(int, time.split(':'))
                email_date = email_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if email_date.date() > TODAY:
                    return None
                return email_date

        result = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        if pd.isna(result):
            return None
        if result.date() > TODAY:
            return None
        return result
    except Exception:
        return None


# =========== Daily Data Prep Functions ==========

def prepare_daily_sales_data(df):
    df_clean = df.copy()
    df_clean["Date and Time"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    df_clean = df_clean.dropna(subset=["Date and Time"])
    df_clean = df_clean[df_clean["Date and Time"].dt.date <= TODAY]
    df_clean["Year"] = df_clean["Date and Time"].dt.year
    df_clean["Month"] = df_clean["Date and Time"].dt.month
    df_clean["Day"] = df_clean["Date and Time"].dt.day
    df_clean["Amount"] = pd.to_numeric(df_clean["Amount"], errors="coerce").fillna(0)
    daily = df_clean.groupby(["Year", "Month", "Day"]).agg({"Amount": "sum"}).reset_index()
    daily["Date"] = pd.to_datetime(daily[["Year", "Month", "Day"]])
    return daily


def prepare_daily_wp_sales_data(df):
    df_clean = df.copy()
    df_clean["cleaned date"] = df_clean["Date"].apply(clean_wp_date)
    df_clean = df_clean.dropna(subset=["cleaned date"])
    df_clean["Total Amount"] = pd.to_numeric(df_clean["Total Amount"], errors="coerce").fillna(0)
    df_clean["Year"] = df_clean["cleaned date"].dt.year
    df_clean["Month"] = df_clean["cleaned date"].dt.month
    df_clean["Day"] = df_clean["cleaned date"].dt.day
    daily = df_clean.groupby(["Year", "Month", "Day"]).agg({"Total Amount": "sum"}).reset_index()
    daily["Date"] = pd.to_datetime(daily[["Year", "Month", "Day"]])
    return daily


def prepare_daily_social_data(df):
    df_clean = df.copy()
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    # Enforce no-future-date cap (belt and braces)
    df_clean = df_clean[df_clean["Parsed_Date"] <= TODAY]
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    df_clean["Day"] = df_clean["Parsed_Date"].dt.day
    df_clean["Post_Count"] = 1
    daily = df_clean.groupby(["Year", "Month", "Day"]).agg({"Post_Count": "sum"}).reset_index()
    daily["Total_Score"] = daily["Post_Count"]
    daily["Date"] = pd.to_datetime(daily[["Year", "Month", "Day"]])
    return daily


def prepare_daily_email_data(df):
    df_clean = df.copy()
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_email_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    df_clean = df_clean[df_clean["Parsed_Date"].dt.date <= TODAY]
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    df_clean["Day"] = df_clean["Parsed_Date"].dt.day
    df_clean["Email_Count"] = 1
    daily = df_clean.groupby(["Year", "Month", "Day"]).agg({"Email_Count": "sum"}).reset_index()
    daily["Date"] = pd.to_datetime(daily[["Year", "Month", "Day"]])
    return daily


# =========== Monthly Data Prep Functions (for analysis) ==========

def prepare_sales_data(df):
    df_clean = df.copy()
    df_clean["Date and Time"] = pd.to_datetime(df_clean["Date and Time"], errors="coerce")
    df_clean = df_clean.dropna(subset=["Date and Time"])
    df_clean = df_clean[df_clean["Date and Time"].dt.date <= TODAY]
    df_clean["Amount"] = pd.to_numeric(df_clean["Amount"], errors="coerce").fillna(0)
    df_clean["Year"] = df_clean["Date and Time"].dt.year
    df_clean["Month"] = df_clean["Date and Time"].dt.month
    monthly = df_clean.groupby(["Year", "Month"]).agg({"Amount": "sum"}).reset_index()
    monthly["Date"] = pd.to_datetime(monthly[["Year", "Month"]].assign(DAY=1))
    monthly["Month_Name"] = monthly["Month"].apply(lambda x: calendar.month_abbr[x])
    return monthly


def prepare_wp_sales_data(df):
    df_clean = df.copy()
    df_clean["cleaned date"] = df_clean["Date"].apply(clean_wp_date)
    df_clean = df_clean.dropna(subset=["cleaned date"])
    df_clean["Total Amount"] = pd.to_numeric(df_clean["Total Amount"], errors="coerce").fillna(0)
    df_clean["Year"] = df_clean["cleaned date"].dt.year
    df_clean["Month"] = df_clean["cleaned date"].dt.month
    monthly = df_clean.groupby(["Year", "Month"]).agg({"Total Amount": "sum"}).reset_index()
    monthly["Date"] = pd.to_datetime(monthly[["Year", "Month"]].assign(DAY=1))
    monthly["Month_Name"] = monthly["Month"].apply(lambda x: calendar.month_abbr[x])
    return monthly


def prepare_social_data(df):
    df_clean = df.copy()
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_social_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean = df_clean[df_clean["Parsed_Date"] <= TODAY]
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    engagement_columns = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    for col in engagement_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).apply(
                lambda x: 0 if "no data available" in x.lower() else x
            )
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    available_eng = [c for c in engagement_columns if c in df_clean.columns]
    df_clean["Total_Score"] = df_clean[available_eng].sum(axis=1) if available_eng else 0
    monthly = df_clean.groupby(["Year", "Month"]).agg({"Total_Score": "sum"}).reset_index()
    monthly["Date"] = pd.to_datetime(monthly[["Year", "Month"]].assign(DAY=1))
    monthly["Month_Name"] = monthly["Month"].apply(lambda x: calendar.month_abbr[x])
    return monthly


def prepare_email_data(df):
    df_clean = df.copy()
    df_clean["Parsed_Date"] = df_clean["Date"].apply(parse_email_date)
    df_clean = df_clean.dropna(subset=["Parsed_Date"])
    df_clean["Parsed_Date"] = pd.to_datetime(df_clean["Parsed_Date"])
    df_clean = df_clean[df_clean["Parsed_Date"].dt.date <= TODAY]
    df_clean["Year"] = df_clean["Parsed_Date"].dt.year
    df_clean["Month"] = df_clean["Parsed_Date"].dt.month
    df_clean["Email_Count"] = 1
    monthly = df_clean.groupby(["Year", "Month"]).agg({"Email_Count": "sum"}).reset_index()
    monthly["Date"] = pd.to_datetime(monthly[["Year", "Month"]].assign(DAY=1))
    monthly["Month_Name"] = monthly["Month"].apply(lambda x: calendar.month_abbr[x])
    return monthly


# =========== Analysis Functions ==========

def analyze_best_posting_times(social_df, sales_df, wp_sales_df):
    recommendations = []
    try:
        social_df_clean = social_df.copy()
        social_df_clean["Parsed_Date"] = social_df_clean["Date"].apply(parse_social_date)
        social_df_clean = social_df_clean.dropna(subset=["Parsed_Date"])
        social_df_clean = social_df_clean[social_df_clean["Parsed_Date"] <= TODAY]
        social_df_clean["Parsed_Date"] = pd.to_datetime(social_df_clean["Parsed_Date"])
        social_df_clean["Month"] = social_df_clean["Parsed_Date"].dt.month
        engagement_columns = ["Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
        for col in engagement_columns:
            if col in social_df_clean.columns:
                social_df_clean[col] = pd.to_numeric(social_df_clean[col], errors='coerce').fillna(0)
        available_eng = [c for c in engagement_columns if c in social_df_clean.columns]
        social_df_clean["Total_Score"] = social_df_clean[available_eng].sum(axis=1) if available_eng else 0
        monthly_social = social_df_clean.groupby("Month").agg({"Total_Score": "mean"}).reset_index()
        if not monthly_social.empty:
            best_social_month = monthly_social.loc[monthly_social["Total_Score"].idxmax(), "Month"]
            recommendations.append(f"Best social media month: {calendar.month_name[best_social_month]} (highest engagement)")
    except Exception:
        recommendations.append("Social media data analysis unavailable")

    try:
        sales_df_clean = sales_df.copy()
        wp_sales_df_clean = wp_sales_df.copy()
        sales_df_clean["Date"] = pd.to_datetime(sales_df_clean["Date and Time"], errors="coerce")
        wp_sales_df_clean["cleaned date"] = wp_sales_df_clean["Date"].apply(clean_wp_date)
        sales_df_clean = sales_df_clean.dropna(subset=["Date"])
        wp_sales_df_clean = wp_sales_df_clean.dropna(subset=["cleaned date"])
        sales_df_clean["Amount"] = pd.to_numeric(sales_df_clean["Amount"], errors="coerce").fillna(0)
        wp_sales_df_clean["Total Amount"] = pd.to_numeric(wp_sales_df_clean["Total Amount"], errors="coerce").fillna(0)
        all_sales = pd.concat([
            sales_df_clean[["Date", "Amount"]].rename(columns={"Amount": "Value"}),
            wp_sales_df_clean[["cleaned date", "Total Amount"]].rename(columns={"cleaned date": "Date", "Total Amount": "Value"})
        ])
        all_sales = all_sales.dropna(subset=["Date"])
        all_sales["Month"] = all_sales["Date"].dt.month
        monthly_sales = all_sales.groupby("Month").agg({"Value": "sum"}).reset_index()
        if not monthly_sales.empty:
            best_sales_month = monthly_sales.loc[monthly_sales["Value"].idxmax(), "Month"]
            recommendations.append(f"Best sales month: {calendar.month_name[best_sales_month]} (highest revenue)")
    except Exception:
        recommendations.append("Sales pattern analysis unavailable")

    try:
        monthly_social_agg = prepare_social_data(social_df)
        monthly_sales_agg = prepare_sales_data(sales_df)
        merged_data = monthly_social_agg.merge(
            monthly_sales_agg[["Year", "Month", "Amount"]],
            on=["Year", "Month"], how="inner"
        )
        if len(merged_data) > 1:
            correlation = merged_data["Total_Score"].corr(merged_data["Amount"])
            if correlation > 0.5:
                recommendations.append("Strong positive correlation between social engagement and sales")
            elif correlation > 0.2:
                recommendations.append("Moderate correlation between social engagement and sales")
            else:
                recommendations.append("Weak correlation between social engagement and sales")
    except Exception:
        recommendations.append("Correlation analysis unavailable")

    return recommendations


def analyze_cross_platform_performance(social_df):
    if social_df.empty:
        return pd.DataFrame()
    df_clean = social_df.copy()
    engagement_columns = ["Likes/Reactions", "Comments", "Impressions", "Shares", "Clicks/Eng. Rate"]
    for col in engagement_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).apply(
                lambda x: 0 if "no data available" in x.lower() or x.lower() == "nan" or x == "" else x
            )
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    if 'Platform' not in df_clean.columns:
        df_clean['Platform'] = 'unknown'
    else:
        df_clean['Platform'] = df_clean['Platform'].fillna('unknown').str.lower()
    available = [c for c in ['Likes/Reactions', 'Comments', 'Impressions'] if c in df_clean.columns]
    df_clean['Total_Score'] = df_clean[available].sum(axis=1) if available else 0
    agg_cols = {c: 'mean' for c in ['Likes/Reactions', 'Comments', 'Impressions'] if c in df_clean.columns}
    agg_cols['Total_Score'] = ['mean', 'max', 'count']
    platform_performance = df_clean.groupby('Platform').agg(agg_cols).round(2)
    platform_performance.columns = ['_'.join(col).strip() for col in platform_performance.columns.values]
    platform_performance = platform_performance.reset_index()
    return platform_performance


def analyze_email_sales_correlation(daily_email, daily_sales, daily_wp_sales):
    recommendations = []
    try:
        if daily_email.empty or (daily_sales.empty and daily_wp_sales.empty):
            recommendations.append("Need more data for email-sales correlation analysis")
            return recommendations
        combined_sales = daily_sales.merge(daily_wp_sales[['Date', 'Total Amount']], on='Date', how='outer')
        combined_sales['Total_Sales'] = combined_sales['Amount'].fillna(0) + combined_sales['Total Amount'].fillna(0)
        merged_data = daily_email.merge(combined_sales[['Date', 'Total_Sales']], on='Date', how='inner')
        if len(merged_data) > 1:
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
                email_days = merged_data[merged_data['Email_Count'] > 0]
                no_email_days = merged_data[merged_data['Email_Count'] == 0]
                if len(email_days) > 0 and len(no_email_days) > 0:
                    avg_with = email_days['Total_Sales'].mean()
                    avg_without = no_email_days['Total_Sales'].mean()
                    if avg_with > avg_without and avg_without > 0:
                        boost = (avg_with - avg_without) / avg_without * 100
                        recommendations.append(f"Sales on email days are {boost:.1f}% higher than non-email days")
                    else:
                        recommendations.append("No significant sales boost observed on email days")
        else:
            recommendations.append("Insufficient overlapping data for email-sales correlation analysis")
    except Exception as e:
        recommendations.append(f"Email-sales correlation analysis limited: {str(e)}")
    return recommendations


def analyze_seasonal_trends(monthly_social, monthly_sales, monthly_wp_sales):
    recommendations = []
    try:
        merged_monthly = monthly_social.merge(
            monthly_sales[['Year', 'Month', 'Amount']], on=['Year', 'Month'], how='outer', suffixes=('_social', '_sales')
        ).merge(monthly_wp_sales[['Year', 'Month', 'Total Amount']], on=['Year', 'Month'], how='outer')
        merged_monthly['Total_Sales'] = merged_monthly['Amount'].fillna(0) + merged_monthly['Total Amount'].fillna(0)
        merged_monthly['Total_Score'] = merged_monthly['Total_Score'].fillna(0)
        monthly_patterns = merged_monthly.groupby('Month').agg({'Total_Sales': 'mean', 'Total_Score': 'mean'}).reset_index()
        if not monthly_patterns.empty:
            best_sales_month = monthly_patterns.loc[monthly_patterns['Total_Sales'].idxmax(), 'Month']
            best_social_month = monthly_patterns.loc[monthly_patterns['Total_Score'].idxmax(), 'Month']
            recommendations.append(f"Seasonal peak sales: {calendar.month_name[best_sales_month]} historically strongest for revenue")
            recommendations.append(f"Seasonal peak engagement: {calendar.month_name[best_social_month]} historically best for social engagement")
            if best_sales_month == best_social_month:
                recommendations.append(f"Perfect alignment: {calendar.month_name[best_sales_month]} is peak for both sales AND engagement!")
            else:
                recommendations.append(f"Consider increasing social activity in {calendar.month_name[best_social_month]} to build momentum for {calendar.month_name[best_sales_month]} sales peak")
    except Exception:
        recommendations.append("Seasonal analysis limited by available data")
    return recommendations


def create_performance_metrics(sales_df, wp_sales_df, social_df, email_df):
    metrics = {}
    try:
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
        if not social_df.empty:
            metrics["total_posts"] = len(social_df)
            metrics["platform_diversity"] = social_df['Platform'].nunique() if 'Platform' in social_df.columns else 0
        if not email_df.empty:
            metrics["total_emails"] = len(email_df)
            if 'Sender' in email_df.columns:
                metrics["training_emails"] = len(email_df[email_df['Sender'].str.contains('training', case=False, na=False)])
            else:
                metrics["training_emails"] = 0
        valid_sales = len(sales_df[sales_df["Amount"].notna()]) if "Amount" in sales_df.columns else 0
        valid_wp_sales = len(wp_sales_df[wp_sales_df["Total Amount"].notna()]) if "Total Amount" in wp_sales_df.columns else 0
        metrics["total_customers"] = valid_sales + valid_wp_sales
    except Exception as e:
        st.error(f"Error calculating metrics: {e}")
        metrics = {
            "total_revenue": 0, "thinkific_revenue": 0, "webinar_revenue": 0,
            "total_customers": 0, "total_posts": 0, "platform_diversity": 0,
            "total_emails": 0, "training_emails": 0
        }
    return metrics


# ====================== Main Application =====================
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Admin Dashboard")

# ====================== Daily Graph Setup =====================
try:
    daily_sales = prepare_daily_sales_data(sales_df)
    daily_wp_sales = prepare_daily_wp_sales_data(wp_sales_df)
    daily_social = prepare_daily_social_data(social_df)
    daily_email = prepare_daily_email_data(email_df)

    monthly_sales = prepare_sales_data(sales_df)
    monthly_wp_sales = prepare_wp_sales_data(wp_sales_df)
    monthly_social = prepare_social_data(social_df)
    monthly_email = prepare_email_data(email_df)

    available_years = sorted(
        set(daily_sales["Year"].unique())
        .union(set(daily_social["Year"].unique()))
        .union(set(daily_wp_sales["Year"].unique()))
        .union(set(daily_email["Year"].unique()))
    )

    selected_year = st.selectbox(
        "Select Year",
        available_years,
        index=len(available_years) - 1 if available_years else 0
    )

    daily_sales_filtered = daily_sales[daily_sales["Year"] == selected_year]
    daily_wp_sales_filtered = daily_wp_sales[daily_wp_sales["Year"] == selected_year]
    daily_social_filtered = daily_social[daily_social["Year"] == selected_year]
    daily_email_filtered = daily_email[daily_email["Year"] == selected_year]

    sales_filtered = monthly_sales[monthly_sales["Year"] == selected_year]
    wp_sales_filtered = monthly_wp_sales[monthly_wp_sales["Year"] == selected_year]
    social_filtered = monthly_social[monthly_social["Year"] == selected_year]
    email_filtered = monthly_email[monthly_email["Year"] == selected_year]

    # Build combined dataframe with a unified "Value" column and Type label
    # Sales and WP sales share the left Y axis (£ amounts)
    # Social post count and email count share the right Y axis (counts)
    sales_plot = daily_sales_filtered[["Date", "Amount"]].rename(columns={"Amount": "Value"}).assign(Type="Thinkific Sales", Axis="£ Revenue")
    wp_plot = daily_wp_sales_filtered[["Date", "Total Amount"]].rename(columns={"Total Amount": "Value"}).assign(Type="Webinar Sales", Axis="£ Revenue")
    social_plot = daily_social_filtered[["Date", "Total_Score"]].rename(columns={"Total_Score": "Value"}).assign(Type="Social Posts", Axis="Count")
    email_plot = daily_email_filtered[["Date", "Email_Count"]].rename(columns={"Email_Count": "Value"}).assign(Type="Emails Sent", Axis="Count")

    combined_daily_data = pd.concat([sales_plot, wp_plot, social_plot, email_plot], ignore_index=True)
    combined_daily_data["Date"] = pd.to_datetime(combined_daily_data["Date"])

except Exception as e:
    st.error(f"Error processing data: {e}")
    available_years = [datetime.now().year]
    selected_year = available_years[0]
    combined_daily_data = pd.DataFrame()
    sales_filtered = pd.DataFrame()
    wp_sales_filtered = pd.DataFrame()
    social_filtered = pd.DataFrame()
    email_filtered = pd.DataFrame()

# ====================== Tabs ======================
tab_main, tab_sales, tab_social, tab_email, tab_payment_count = st.tabs([
    "📈 Analytics", "Sales Data", "Social Data", "Email Marketing", "Sales by User"
])

# ====================== Analytics Tab ======================
with tab_main:
    st.header(f"Daily Performance ({selected_year})")

    if not combined_daily_data.empty:

        # --- Revenue lines (left Y axis) ---
        revenue_data = combined_daily_data[combined_daily_data["Axis"] == "£ Revenue"]
        count_data = combined_daily_data[combined_daily_data["Axis"] == "Count"]

        base_revenue = alt.Chart(revenue_data).encode(
            x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45))
        )

        thinkific_line = base_revenue.transform_filter(
            alt.datum.Type == "Thinkific Sales"
        ).mark_line(strokeWidth=2).encode(
            y=alt.Y("Value:Q", title="£ Revenue", scale=alt.Scale(zero=False)),
            color=alt.value("#4C8BF5"),
            tooltip=[
                alt.Tooltip("Date:T", format="%d %b %Y"),
                alt.Tooltip("Value:Q", title="Thinkific £", format=",.2f")
            ]
        )

        wp_line = base_revenue.transform_filter(
            alt.datum.Type == "Webinar Sales"
        ).mark_line(strokeWidth=2).encode(
            y=alt.Y("Value:Q", scale=alt.Scale(zero=False)),
            color=alt.value("#2ECC71"),
            tooltip=[
                alt.Tooltip("Date:T", format="%d %b %Y"),
                alt.Tooltip("Value:Q", title="Webinar £", format=",.2f")
            ]
        )

        revenue_chart = alt.layer(thinkific_line, wp_line)

        # --- Count dots (right Y axis) ---
        base_count = alt.Chart(count_data).encode(
            x=alt.X("Date:T", axis=alt.Axis(format="%b %d", labelAngle=-45))
        )

        social_dots = base_count.transform_filter(
            alt.datum.Type == "Social Posts"
        ).mark_circle(size=50).encode(
            y=alt.Y("Value:Q", title="Count", scale=alt.Scale(zero=False)),
            color=alt.value("#E74C3C"),
            tooltip=[
                alt.Tooltip("Date:T", format="%d %b %Y"),
                alt.Tooltip("Value:Q", title="Posts")
            ]
        )

        email_dots = base_count.transform_filter(
            alt.datum.Type == "Emails Sent"
        ).mark_circle(size=50).encode(
            y=alt.Y("Value:Q", scale=alt.Scale(zero=False)),
            color=alt.value("#F39C12"),
            tooltip=[
                alt.Tooltip("Date:T", format="%d %b %Y"),
                alt.Tooltip("Value:Q", title="Emails")
            ]
        )

        count_chart = alt.layer(social_dots, email_dots)

        # --- Combine with independent Y axes ---
        combined_chart = alt.layer(revenue_chart, count_chart).resolve_scale(
            y="independent"
        ).properties(
            width=800,
            height=420,
            title=f"Sales, Social Posts & Emails — {selected_year}"
        )

        st.altair_chart(combined_chart, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("🔵 **Blue line**: Thinkific Sales (£)")
        with col2:
            st.markdown("🟢 **Green line**: Webinar Sales (£)")
        with col3:
            st.markdown("🔴 **Red dots**: Social Posts (count)")
        with col4:
            st.markdown("🟠 **Orange dots**: Emails Sent (count)")

        st.divider()

        # Yearly Metrics
        st.subheader(f"Yearly Metrics ({selected_year})")
        col1, col2, col3 = st.columns(3)
        yearly_thinkific_sales = sales_filtered["Amount"].sum() if not sales_filtered.empty and "Amount" in sales_filtered.columns else 0
        yearly_wp_sales = wp_sales_filtered["Total Amount"].sum() if not wp_sales_filtered.empty and "Total Amount" in wp_sales_filtered.columns else 0
        yearly_total_revenue = yearly_thinkific_sales + yearly_wp_sales
        with col1:
            st.metric("Total Revenue", f"£{yearly_total_revenue:,.2f}")
        with col2:
            st.metric("Thinkific Revenue", f"£{yearly_thinkific_sales:,.2f}")
        with col3:
            st.metric("Webinar Revenue", f"£{yearly_wp_sales:,.2f}")

        st.divider()

        metrics = create_performance_metrics(sales_df, wp_sales_df, social_df, email_df)

        st.header("Analytics (all data)")
        st.subheader("Total Metrics")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Revenue", f"£{round(metrics.get('total_revenue', 0), 2):,.2f}")
        with col2:
            st.metric("Thinkific Revenue", f"£{round(metrics.get('thinkific_revenue', 0), 2):,.2f}")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Webinar Revenue", f"£{round(metrics.get('webinar_revenue', 0), 2):,.2f}")
        with col2:
            st.metric("Total Customers", f"{metrics.get('total_customers', 0)}")

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
        st.warning("No data available to display. Check your sheet connections.")

# ====================== Other Tabs ======================
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

    if "Email address" in sales_df.columns:
        sales_df["Amount"] = pd.to_numeric(sales_df["Amount"], errors='coerce').fillna(0)
        thinkific_summary = sales_df.groupby("Email address").agg({"Amount": ["sum", "count"]}).reset_index()
        thinkific_summary.columns = ["Email address", "Amount Spent", "Purchase Count"]
        thinkific_summary = thinkific_summary[["Email address", "Purchase Count", "Amount Spent"]]
        st.subheader("Thinkific Sales")
        st.write(thinkific_summary)
    else:
        st.warning("Thinkific sales data: 'Email address' column not found")
        thinkific_summary = pd.DataFrame()

    if "Email" in wp_sales_df.columns:
        wp_sales_df["Total Amount"] = pd.to_numeric(wp_sales_df["Total Amount"], errors='coerce').fillna(0)
        wp_summary = wp_sales_df.groupby("Email").agg({"Total Amount": ["sum", "count"]}).reset_index()
        wp_summary.columns = ["Email", "Amount Spent", "Purchase Count"]
        wp_summary = wp_summary[["Email", "Purchase Count", "Amount Spent"]]
        st.subheader("Live Webinar Sales")
        st.write(wp_summary)
    else:
        st.warning("WordPress sales data: 'Email' column not found")
        wp_summary = pd.DataFrame()

    if not thinkific_summary.empty and not wp_summary.empty:
        st.subheader("Combined Data")
        combined_df = pd.concat([
            thinkific_summary.rename(columns={"Email address": "Email"}),
            wp_summary
        ], ignore_index=True)
        final_combined = combined_df.groupby("Email").agg({
            "Purchase Count": "sum",
            "Amount Spent": "sum"
        }).reset_index().sort_values("Amount Spent", ascending=False).reset_index(drop=True)
        st.write(final_combined)
    elif not thinkific_summary.empty or not wp_summary.empty:
        st.info("Only one dataset available for combination")
    else:
        st.warning("No sales data available for analysis")