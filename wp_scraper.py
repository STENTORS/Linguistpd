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
driver.get("")

input("Log in to WP, then press Enter here...")

time.sleep(5)



driver.quit()

# --- Upload to Google Sheets ---
sheet.clear()
sheet.update("A1", data)
print(f"✅ Uploaded {len(data)-1} posts to Google Sheet: {SHEET_NAME}")
