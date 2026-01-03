
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

buffer_user = os.environ["buffer_user"]
buffer_pass = os.environ["buffer_pass"]

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "LinguistPd Buffer Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
def create_keyfile_dict():
            variables_keys = {
                "type": os.environ.get("SHEET_TYPE"),
                "project_id": os.environ.get("SHEET_PROJECT_ID"),
                "private_key_id": os.environ.get("SHEET_PRIVATE_KEY_ID"),
                "private_key": os.environ.get("SHEET_PRIVATE_KEY"),
                "client_email": os.environ.get("SHEET_CLIENT_EMAIL"),
                "client_id": os.environ.get("SHEET_CLIENT_ID"),
                "auth_uri": os.environ.get("SHEET_AUTH_URI"),
                "token_uri": os.environ.get("SHEET_TOKEN_URI"),
                "auth_provider_x509_cert_url": os.environ.get("SHEET_AUTH_PROVIDER_X509_CERT_URL"),
                "client_x509_cert_url": os.environ.get("SHEET_CLIENT_X509_CERT_URL"),
            }
            return variables_keys


def parse_date(date_str):
    """
    Convert dates like "Friday, 18 July" or "Monday, 30 December 2024" to dd/mm/yyyy
    Assumes current year if year not specified
    """
    try:
        try:
            date_part = date_str.split(', ', 1)[1]
            dt = datetime.strptime(date_part, "%d %B")
            current_year = datetime.now().year
            dt = dt.replace(year=current_year)
        except:
            date_part = date_str.split(", ")[1]
            dt = datetime.strptime(date_part, "%d %B %Y")
        # Format as dd/mm/yyyy
        return dt.strftime("%d/%m/%Y")
    except:
        return(date_str)

def get_last_date(sheet):
    all_rows = sheet.get_all_values()
    last_row = all_rows[-1]
    last_date = last_row[0]
    return parse_date(last_date)
    
def login():
    reject_cookies = wait.until(
        EC.element_to_be_clickable((By.CLASS_NAME, "cky-btn-reject"))
    )

    reject_cookies.click()

    username_field = driver.find_element(By.NAME, "email")
    password_field = driver.find_element(By.NAME, "password")

    # Clear fields and enter credentials
    username_field.clear()
    username_field.send_keys(buffer_user)

    password_field.clear()
    password_field.send_keys(buffer_pass)
    enter_login = driver.find_element(By.ID, "login-form-submit")
    enter_login.click()


creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
driver.get("https://publish.buffer.com/all-channels?tab=sent")

wait = WebDriverWait(driver, 20)

try:
    login()
except Exception as e:
    print(f"Login error: {e}")

last_height = driver.execute_script("return document.body.scrollHeight")
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(10)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height


# --- MAIN SCRAPE FUNTIONALITY  ---

last_date = get_last_date(sheet)
wrapper = driver.find_element(By.CLASS_NAME, "publish_timeline_qL9zu")
elements = wrapper.find_elements(By.XPATH, "./*")

data = []
current_date = None

for block in elements:
    class_name = block.get_attribute("class")

    # Date header
    new_post = 0
    if "publish_base_Y1USt" in class_name:
        current_date = block.text.strip()
        if parse_date(current_date) > last_date:
            new_post += 1

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
                    parse_date(current_date),
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
for row in data:
    sheet.append_row(row)
print(f"✅ Uploaded {len(data)} posts")
