import streamlit as st
from streamlit_gsheets import GSheetsConnection

# Create a connection object.
conn = st.connection("gsheets", type=GSheetsConnection)

df = conn.read()
st.logo("lpd-logo.png", size="large")
st.title("Linguistpd Sales Analytics")
# Print results.
for row in df.itertuples():
    st.write(f"{row.Date} | {row.Platform} | {row.Likes} | {row.Comments} | {row.Reposts} | {row.Sales}")
