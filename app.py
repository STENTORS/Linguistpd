import streamlit as st
from streamlit_gsheets import GSheetsConnection

# Connect to the public sheet
social_conn = st.connection("social_gsheets", type=GSheetsConnection)
social_df = social_conn.read()

# Connect to the private sheet
sales_conn = st.connection("thinkific_gsheets", type=GSheetsConnection)
sales_df = sales_conn.read()


#========Header========
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Sales Analytics")


# ========Content========
tab_main, tab_sales, tab_social = st.tabs(["📈 Analytics", "🗃 Sales Data", "🗃 Social Data"])
with tab_main:
    st.header("Sales x Post by Date")
    options = ["Sales", "Posts", "Limited Time Sales"]
    selection = st.pills("Graph View", options, selection_mode="multi")
    sale_date_range = st.date_input("Date Range of Sale")

with tab_sales:
    st.header("Sales Data")
    st.dataframe(sales_df)

with tab_social:
    st.header("Social Media Data")
    st.dataframe(social_df)

st.divider()

st.button("Run Scraper")
