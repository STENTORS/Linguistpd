import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "WordPress Sales Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
# options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
driver.get("https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list")

input("Log in to WP, then press Enter here...")

time.sleep(5)

wp_payment_data = [["Order ID", "First Name", "Last Name", "Email", "Total Amount", "Status", "Date", "Order"]]

table = driver.find_elements(By.CLASS_NAME, "iedit")

for row in table:
    order_id = row.find_element(By.CLASS_NAME, "title").text.strip()
    f_name = row.find_element(By.CLASS_NAME, "wpsc_first_name").text.strip()
    l_name = row.find_element(By.CLASS_NAME, "wpsc_last_name").text.strip()
    email = row.find_element(By.CLASS_NAME, "wpsc_email_address").text.strip()
    amount = row.find_element(By.CLASS_NAME, "wpsc_total_amount").text.strip()
    payment_status = row.find_element(By.CLASS_NAME, "wpsc_order_status").text.strip()
    date = row.find_element(By.CLASS_NAME, "date").text.strip()

    wp_payment_data.append([order_id, f_name, l_name, email, amount, payment_status, date, "N/A"])

driver.quit()

# --- Upload to Google Sheets ---
sheet.clear()
sheet.update("A1", wp_payment_data)