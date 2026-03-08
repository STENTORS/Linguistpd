import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

BUFFER_USER = os.getenv("buffer_user")
BUFFER_PASS = os.getenv("buffer_pass")

if not BUFFER_USER or not BUFFER_PASS:
    raise ValueError("buffer_user or buffer_pass not set in .env file")

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "LinguistPd Buffer Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1


def parse_sheet_date(date_str):
    """Parse sheet date format: '24/04/2023' -> datetime"""
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y")
    except Exception:
        return None


def parse_buffer_date(date_str):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cleaned = date_str.strip()

    # Handle relative prefixes: "Yesterday, 5 March" / "Today, 6 March"
    lower = cleaned.lower()
    if lower.startswith("today"):
        return today
    if lower.startswith("yesterday"):
        return today - timedelta(days=1)

    # Strip leading weekday if present: "Monday 24 April 2023" -> "24 April 2023"
    # Weekdays followed by a space then a digit
    import re
    cleaned = re.sub(r'^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[,\s]+', '', cleaned, flags=re.IGNORECASE)

    # Try formats that include a year
    for fmt in ("%d %B %Y", "%B %d, %Y", "%d %B, %Y"):
        try:
            return datetime.strptime(cleaned, fmt)
        except Exception:
            continue

    # No year present — e.g. "5 March" or "March 5"
    # Try parsing with the current year; if that gives a future date, use previous year
    for fmt in ("%d %B", "%B %d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            candidate = parsed.replace(year=today.year)
            if candidate > today:
                candidate = candidate.replace(year=today.year - 1)
            return candidate
        except Exception:
            continue

    print(f"  [WARN] Could not parse date: '{date_str}'")
    return None


def get_latest_date_from_sheet():
    """
    Sheet stores newest first, so row 1 (index 0) after the header is the most recent.
    Returns a datetime or None.
    """
    try:
        all_records = sheet.get_all_records()
        if not all_records:
            print("Sheet is empty. Will scrape all available posts.")
            return None

        latest_record = all_records[0]
        raw_date = str(latest_record.get('Date', '')).strip()
        latest_date = parse_sheet_date(raw_date)

        print(f"Latest date in sheet: {raw_date} -> parsed as {latest_date}")
        return latest_date

    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        return None


def login_to_buffer(driver, wait):
    """Automatically log in to Buffer using credentials from .env"""
    print("Logging in to Buffer...")
    driver.get("https://login.buffer.com/login?redirect=https%3A%2F%2Fpublish.buffer.com%2F")

    wait.until(EC.presence_of_element_located((By.ID, "email")))

    driver.find_element(By.ID, "email").send_keys(BUFFER_USER)
    driver.find_element(By.ID, "password").send_keys(BUFFER_PASS)
    driver.find_element(By.ID, "login-form-submit").click()

    print("Waiting for Buffer dashboard to load...")
    wait.until(EC.url_contains("publish.buffer.com"))
    time.sleep(5)
    print("Logged in successfully.")


def initial_page_load_scroll(driver):
    """
    After the page first loads, do a deliberate scroll -> 10s wait -> scroll again
    to ensure the initial batch of posts is fully rendered before we start scraping.
    """
    print("Initial page load scroll...")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    print("  Scrolled down — waiting 10s for content to load...")
    time.sleep(10)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    print("  Scrolled again — waiting 5s...")
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, 0);")  # Back to top before scraping loop
    time.sleep(2)
    print("Initial load scroll complete.")


def scroll_until_stable(driver, latest_known_date):
    """
    Scroll to the bottom in a loop.
    Each iteration: scroll -> wait 10s -> scroll again -> wait 3s -> check height.
    Stops when height is stable across 2 consecutive passes, or we've passed the cutoff date.
    """
    print("Starting scroll to load all relevant posts...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    passes_without_change = 0

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print("  Scrolled — waiting 10s...")
        time.sleep(10)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        new_height = driver.execute_script("return document.body.scrollHeight")

        if new_height == last_height:
            passes_without_change += 1
            if passes_without_change >= 2:
                print("Page fully loaded (height stable across 2 passes).")
                break
        else:
            passes_without_change = 0
            last_height = new_height
            print(f"  New content loaded (height: {new_height}px). Continuing scroll...")

        # Early exit if we've scrolled past the cutoff date
        if latest_known_date:
            try:
                date_headers = driver.find_elements(By.XPATH,
                    "//*[contains(@class, 'publish_base_Y1USt')]")
                if date_headers:
                    oldest_visible_text = date_headers[-1].text.strip()
                    oldest_visible_date = parse_buffer_date(oldest_visible_text)
                    if oldest_visible_date and oldest_visible_date <= latest_known_date:
                        print(f"  Scrolled past cutoff date ({oldest_visible_text}). Stopping scroll.")
                        break
            except Exception:
                pass

    print("Scroll complete.")


# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
options.add_argument("--headless")
driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 30)

# Auto login
login_to_buffer(driver, wait)

# Navigate to sent posts
driver.get("https://publish.buffer.com/all-channels?tab=sent")
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "publish_timeline_qL9zu")))
time.sleep(3)

# Get cutoff date from sheet
latest_known_date = get_latest_date_from_sheet()

# Step 1: deliberate initial scroll to ensure first batch loads fully
initial_page_load_scroll(driver)

# Step 2: keep scrolling until all new posts are loaded
scroll_until_stable(driver, latest_known_date)

# ---------------- SCRAPE ----------------
wrapper = driver.find_element(By.CLASS_NAME, "publish_timeline_qL9zu")
elements = wrapper.find_elements(By.XPATH, "./*")

new_data = []
current_date = None
current_date_parsed = None
stop_scraping = False

for block in elements:
    if stop_scraping:
        break

    class_name = block.get_attribute("class")

    # Date header block
    if "publish_base_Y1USt" in class_name:
        current_date_text = block.text.strip()
        current_date_parsed = parse_buffer_date(current_date_text)

        if current_date_parsed:
            current_date = current_date_parsed.strftime("%d/%m/%Y")
            print(f"Date header: '{current_date_text}' -> {current_date}")
        else:
            current_date = current_date_text  # fallback raw

        # Stop if we've hit the cutoff
        if latest_known_date and current_date_parsed and current_date_parsed <= latest_known_date:
            print(f"Reached cutoff date ({current_date}). Stopping scrape.")
            stop_scraping = True
        continue

    # Post container
    if "publish_postContainer" in class_name or "publish_wrapper_KDBT-" in class_name:
        if stop_scraping:
            break

        try:
            platform_elem = block.find_element(By.CSS_SELECTOR, 'div[data-channel]')
            platform_info = platform_elem.get_attribute("data-channel")
        except Exception:
            platform_info = ""

        try:
            time_text = block.find_element(By.CLASS_NAME, "publish_labelContainer_NIys3").text.strip()
        except Exception:
            time_text = ""

        try:
            post_text = block.find_element(By.CLASS_NAME, "publish_body_oZVDR").text.strip()
        except Exception:
            post_text = ""

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
                except Exception:
                    pass
        except Exception:
            pass

        try:
            likes_reactions = int(metrics.get("Likes") or metrics.get("Reactions") or 0)
        except Exception:
            likes_reactions = 0

        try:
            clicks_eng = int(metrics.get("Clicks") or metrics.get("Eng. Rate") or 0)
        except Exception:
            clicks_eng = 0

        try:
            comment_val = int(metrics.get("Comments") or 0)
        except Exception:
            comment_val = 0

        try:
            impression_val = int(metrics.get("Impressions") or 0)
        except Exception:
            impression_val = 0

        try:
            share_val = int(metrics.get("Shares") or 0)
        except Exception:
            share_val = 0

        social_score = likes_reactions + clicks_eng + comment_val + impression_val + share_val

        new_data.append([
            current_date,
            time_text,
            platform_info,
            post_text,
            likes_reactions,
            metrics.get("Comments", ""),
            metrics.get("Impressions", ""),
            metrics.get("Shares", ""),
            clicks_eng,
            social_score
        ])

driver.quit()

print(f"\nScraped {len(new_data)} new post(s).")

# ---------------- UPDATE GOOGLE SHEETS ----------------
if not new_data:
    print("No new posts found. Sheet unchanged.")
else:
    if latest_known_date is None:
        full_data = [["Date", "Time", "Platform", "Post", "Likes/Reactions", "Comments",
                       "Impressions", "Shares", "Clicks/Eng. Rate", "Total Social Score"]]
        full_data.extend(new_data)
        sheet.clear()
        sheet.update("A1", full_data)
        print(f"✅ Fresh upload: {len(new_data)} posts written to '{SHEET_NAME}'.")
    else:
        sheet.insert_rows(new_data, row=2)
        print(f"✅ Prepended {len(new_data)} new post(s) to '{SHEET_NAME}' (newest first).")