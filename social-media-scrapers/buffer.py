import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "LinguistPd Buffer Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
driver.get("https://publish.buffer.com/all-channels?tab=sent")

input("Log in to Buffer, then press Enter here...")

time.sleep(5)

# --- Auto scroll ---
last_height = driver.execute_script("return document.body.scrollHeight")
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height

print("✅ Finished scrolling")

# --- Scrape ---
wrapper = driver.find_element(By.CLASS_NAME, "publish_timeline_qL9zu")
elements = wrapper.find_elements(By.XPATH, "./*")

# Header row
data = [["Date", "Time", "Platform", "Post", "Likes/Reactions", "Comments", "Impressions", "Shares", "Clicks/Eng. Rate", "Total Social Score"]]
current_date = None

for block in elements:
    class_name = block.get_attribute("class")

    # Date header
    if "publish_base_Y1USt" in class_name:
        current_date = block.text.strip()
        continue

    # Post container
    if "publish_postContainer" in class_name or "publish_wrapper_KDBT-" in class_name:
        try:
            platform_elem = block.find_element(By.CSS_SELECTOR, 'div[data-channel]')
            platform_info = platform_elem.get_attribute("data-channel")
        except:
            platform_info = ""
        try:
            time_text = block.find_element(By.CLASS_NAME, "publish_labelContainer_NIys3").text.strip()
        except:
            time_text = ""
        try:
            post_text = block.find_element(By.CLASS_NAME, "publish_body_oZVDR").text.strip()
        except:
            post_text = ""
        try:
            platform = block.find_element(By.CLASS_NAME, "publish_channelName_MobA0").text.strip()
        except:
            platform = ""

        # Metrics dictionary
        metrics = {
            "Likes": "",
            "Reactions": "",
            "Comments": "",
            "Impressions": "",
            "Shares": "",
            "Clicks": "",
            "Eng. Rate": ""
        }

        try:
            metric_wrappers = block.find_elements(By.CLASS_NAME, "publish_wrapper_6Zayg")
            for m in metric_wrappers:
                try:
                    label = m.find_element(By.CLASS_NAME, "publish_label_79dYt").text.strip()
                    value = m.find_element(By.CLASS_NAME, "publish_metric_3fmE3").text.strip()

                    
                    metrics[label] = value
                    
                except:
                    pass
        except:
            pass

        # Merge Likes/Reactions into one column for sheet
        try:
            
            likes_reactions = int(metrics.get("Likes")) or int(metrics.get("Reactions"))
        except:
            likes_reactions = 0
        try:
            clicks_eng = int(metrics.get("Clicks")) or int(metrics.get("Eng. Rate"))
        except:
            clicks_eng = 0

        try:
            comment_val = int(metrics.get("Comments"))
        except:
            comment_val = 0

        try:
            immpression_val = int(metrics.get("Impressions"))
        except:
            immpression_val = 0

        try:
            share_val = int(metrics.get("Shares"))
        except:
            share_val = 0
       

        social_score = likes_reactions + clicks_eng + comment_val + immpression_val + share_val
     
        data.append([
            current_date,
            time_text,
            platform_info,
            post_text,
            likes_reactions,
            metrics.get("Comments"),
            metrics.get("Impressions"),
            metrics.get("Shares"),
            clicks_eng,
            social_score
        ])

driver.quit()

# --- Upload to Google Sheets ---
sheet.clear()
sheet.update("A1", data)
print(f"✅ Uploaded {len(data)-1} posts to Google Sheet: {SHEET_NAME}")
