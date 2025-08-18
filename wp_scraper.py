import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------
"""
SHEET_NAME = "WordPress Sales Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1
"""

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
# options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
driver.get("https://linguistpd.co.uk/wp-admin/edit.php?post_type=paypal_ipn")

input("Log in to WP, then press Enter here...")

time.sleep(5)

wp_payment_data = [["Transaction ID", "Invoice ID", "Date", "Customer Name", "Amount", "Transaction Type", "Payment Status"]]

table = driver.find_element(By.CLASS_NAME, "wp-list-table")
row = driver.find_element(By.CLASS_NAME, "iedit")

for row in table:
    transation_id = row.find_element(By.CLASS_NAME, "row-title").text.strip()
    invoice_id = row.find_element(By.CLASS_NAME, "invoice").text.strip()
    date = row.find_element(By.CLASS_NAME, "payment_date").text.strip()
    customer_name = row.find_element(By.CLASS_NAME, "first_name").text.strip()
    amount = row.find_element(By.CLASS_NAME, "mc_gross").text.strip()
    transaction_type = row.find_element(By.CLASS_NAME, "txn_type").text.strip()
    payment_status = row.find_element(By.CLASS_NAME, "payment_status").text.strip()

    wp_payment_data.append([transation_id, invoice_id, date, customer_name, amount, transaction_type, payment_status])
driver.quit()

print(wp_payment_data)


# --- Upload to Google Sheets ---
"""
sheet.clear()
sheet.update("A1", data)
print(f"✅ Uploaded {len(data)-1} posts to Google Sheet: {SHEET_NAME}")
"""