import time
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# Get WordPress credentials from .env
WP_USERNAME = os.getenv('WP_USERNAME')
WP_PASSWORD = os.getenv('WP_PASSWORD')

# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "WordPress Sales Data"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
#options.add_argument("--headless")
driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

# Store the specific page URL for latest orders
PAGE_1_URL = "https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list&paged=1"


def parse_wp_date(date_str):

    try:
        # Split on newlines and take the last non-empty part
        # This handles the case where "Published" and the date are on separate lines
        parts = [p.strip() for p in date_str.strip().splitlines() if p.strip()]
        cleaned = parts[-1]  # The date is always the last line

        # Also strip any remaining status prefix (for the no-newline format)
        for prefix in ("Published", "Scheduled", "Pending", "Draft", "Private"):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):].strip()
                break

        return datetime.strptime(cleaned, "%Y/%m/%d at %H:%M")
    except Exception:
        return None


# ---------------- AUTOMATIC WORDPRESS LOGIN ----------------
def login_to_wordpress():
    """Automatically log in to WordPress"""
    print("Logging in to WordPress...")

    driver.get(PAGE_1_URL)
    wait.until(EC.presence_of_element_located((By.ID, "user_login")))

    username_field = driver.find_element(By.ID, "user_login")
    password_field = driver.find_element(By.ID, "user_pass")

    username_field.clear()
    username_field.send_keys(WP_USERNAME)

    password_field.clear()
    password_field.send_keys(WP_PASSWORD)

    try:
        driver.find_element(By.ID, "wp-submit").click()
    except Exception:
        driver.find_element(By.ID, "cn-accept-cookie").click()
        time.sleep(2)
        driver.find_element(By.ID, "wp-submit").click()


def get_last_order_from_sheet():
    """
    Get the last order ID AND date from the Google Sheet.
    Returns (last_order_id_str, last_order_datetime) or (None, None).
    """
    try:
        all_records = sheet.get_all_records()

        if not all_records:
            print("No existing records found in sheet. Starting fresh.")
            return None, None

        last_record = all_records[-1]

        # Always compare as string to avoid int/str mismatch from gspread
        last_order_id = str(last_record['Order ID']).strip()

        # Parse date for fallback comparison
        raw_date = str(last_record.get('Date', '')).strip()
        last_order_date = parse_wp_date(raw_date)

        print(f"Last order in sheet  ->  ID: {last_order_id}  |  Date: {raw_date}")
        return last_order_id, last_order_date

    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return None, None


def scrape_new_orders_from_page(last_known_order_id, last_known_date):
    """
    Scrape only new orders from Page 1 (WordPress lists newest first).
    Stops as soon as we hit an order whose ID matches last_known_order_id
    OR whose date is <= last_known_date (whichever fires first).
    Returns rows in CHRONOLOGICAL order (oldest-first) ready to append.
    """
    new_orders_data = []

    try:
        table = driver.find_elements(By.CLASS_NAME, "iedit")
        print(f"Found {len(table)} orders on page 1")

        main_window = driver.window_handles[0]

        for index in range(len(table)):
            try:
                # Re-find rows to avoid stale element references
                current_table = driver.find_elements(By.CLASS_NAME, "iedit")
                row = current_table[index]

                # Order ID — always treat as string
                order_id = str(row.find_element(By.CLASS_NAME, "title").text.strip())

                # Date
                date_raw = row.find_element(By.CLASS_NAME, "date").text.strip()
                order_date = parse_wp_date(date_raw)

                # Stop condition 1: ID match
                if last_known_order_id and order_id == last_known_order_id:
                    print(f"Reached last known order ID {order_id}. Stopping.")
                    break

                # Stop condition 2: Date is at or before the last known date
                if last_known_date and order_date and order_date <= last_known_date:
                    print(f"Order {order_id} date ({date_raw}) is not newer than last known. Stopping.")
                    break

                f_name = row.find_element(By.CLASS_NAME, "wpsc_first_name").text.strip()
                l_name = row.find_element(By.CLASS_NAME, "wpsc_last_name").text.strip()
                email = row.find_element(By.CLASS_NAME, "wpsc_email_address").text.strip()
                payment_status = row.find_element(By.CLASS_NAME, "wpsc_order_status").text.strip()

                print(f"Processing new order {index + 1}: ID={order_id}  Date={date_raw}")

                # Open order detail in a new tab
                order_detail_url = (
                    f"https://linguistpd.co.uk/wp-admin/post.php?post={order_id}&action=edit"
                )
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(order_detail_url)

                order = "N/A"
                amount = "N/A"
                try:
                    order_element = wait.until(
                        EC.presence_of_element_located((By.NAME, "wpsc_items_ordered"))
                    )
                    total_element = wait.until(
                        EC.presence_of_element_located((By.NAME, "wpsc_total_amount"))
                    )
                    order = order_element.text.strip()
                    amount = total_element.get_attribute("value").strip()
                    print(f"  Items: {len(order)} chars  |  Total: {amount}")
                except Exception as detail_error:
                    print(f"  Could not get order details: {detail_error}")

                # Close tab and return to list
                driver.close()
                driver.switch_to.window(main_window)

                new_orders_data.append(
                    [order_id, f_name, l_name, email, amount, payment_status, date_raw, order]
                )
                print(f"  Collected order {index + 1}")

            except Exception as e:
                print(f"Error processing row {index}: {e}")
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.close()
                driver.switch_to.window(main_window)
                continue

    except Exception as e:
        print(f"Error finding orders table: {e}")
        print(f"Current URL: {driver.current_url}")

    # WordPress lists newest-first; reverse so we append oldest-first (chronological)
    new_orders_data.reverse()
    return new_orders_data


def append_to_sheet(new_orders_data):
    """Append new orders to the Google Sheet in chronological order."""
    if not new_orders_data:
        print("No new orders to append.")
        return

    try:
        sheet.append_rows(new_orders_data)
        print(f"Successfully appended {len(new_orders_data)} new orders to the sheet.")
    except Exception as e:
        print(f"Error appending to Google Sheet: {e}")


# ---------------- MAIN EXECUTION ----------------

# Check the last order in the existing sheet (ID + date)
last_known_order_id, last_known_date = get_last_order_from_sheet()

# Log in to WordPress
login_to_wordpress()

# Wait until the orders table is visible (confirms successful login)
loggedin = False
while not loggedin:
    try:
        driver.find_element(By.CLASS_NAME, "iedit")
        loggedin = True
    except Exception:
        time.sleep(5)

new_orders_data = []

try:
    print("=== CHECKING FOR NEW ORDERS ON PAGE 1 ===")
    driver.get(PAGE_1_URL)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "iedit")))

    new_orders_data = scrape_new_orders_from_page(last_known_order_id, last_known_date)

    if new_orders_data:
        print(f"\nFound {len(new_orders_data)} new order(s) to append.")
    else:
        print("No new orders found since last scrape.")

except Exception as e:
    print(f"Error during scraping process: {e}")

finally:
    driver.quit()

# Append to Google Sheets
if new_orders_data:
    try:
        append_to_sheet(new_orders_data)
        print(f"\n=== UPDATE COMPLETE ===")
        print(f"Successfully added {len(new_orders_data)} new order(s) to the spreadsheet!")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")
else:
    print("\n=== NO UPDATES NEEDED ===")
    print("No new orders found since last scrape.")