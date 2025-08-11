import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "LinguistPd"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1  # First tab

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
driver = webdriver.Firefox(options=options)
driver.get("https://publish.buffer.com/all-channels?tab=sent")

input("Log in to Buffer, then press Enter here...")

time.sleep(5)  # Initial load

# --- Auto scroll to load all posts ---
last_height = driver.execute_script("return document.body.scrollHeight")
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height

print("✅ Finished scrolling")

# --- Scrape posts ---
wrapper = driver.find_element(By.CLASS_NAME, "publish_timeline_qL9zu")
elements = wrapper.find_elements(By.XPATH, "./*")

data = [["Date", "Time", "Post", "Likes/Reactions", "Comments", "Impressions", "Shares", "Clicks/Engagment"]]  # Header row
current_date = None

for block in elements:
    class_name = block.get_attribute("class")

    if "publish_base_Y1USt" in class_name:
        current_date = block.text.strip()
        continue

    if "publish_postContainer" in class_name or "publish_wrapper_KDBT-" in class_name:
        try:
            time_text = block.find_element(By.CLASS_NAME, "publish_labelContainer_NIys3").text.strip()
        except:
            time_text = ""
        try:
            post_text = block.find_element(By.CLASS_NAME, "publish_body_oZVDR").text.strip()
        except:
            post_text = ""
        try:
            metrics_info = block.find_element(By.CLASS_NAME, "publish_metricsWrapper_vZFc6").text.strip()
        except:
            metrics_info = ""

        data.append([current_date, time_text, post_text, metrics_info])

driver.quit()

# --- Upload to Google Sheets ---
sheet.clear() 
sheet.update("A1", data)

print(f"✅ Uploaded {len(data)-1} posts to Google Sheet: {SHEET_NAME}")
