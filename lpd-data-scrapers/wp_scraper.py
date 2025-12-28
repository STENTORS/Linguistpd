import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

WP_USERNAME = os.environ["WP_USERNAME"]
WP_PASSWORD = os.environ["WP_PASSWORD"]


# ---------------- GOOGLE SHEETS SETUP ----------------
SHEET_NAME = "WordPress Sales Data"
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


creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), SCOPE)
client = gspread.authorize(creds)


sheet = client.open(SHEET_NAME).sheet1

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

# Store the specific page URL for latest orders
PAGE_1_URL = "https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list&paged=1"

# ---------------- AUTOMATIC WORDPRESS LOGIN ----------------
def login_to_wordpress():
    """Automatically log in to WordPress"""
    print("Logging in to WordPress...")
    
    # Go to login page using page 1 URL
    driver.get(PAGE_1_URL)
    
    # Wait for login form to load
    wait.until(EC.presence_of_element_located((By.ID, "user_login")))
    
    # Find login fields using the correct selectors from your HTML
    username_field = driver.find_element(By.ID, "user_login")
    password_field = driver.find_element(By.ID, "user_pass")
    
    # Clear fields and enter credentials
    username_field.clear()
    username_field.send_keys(WP_USERNAME)
    
    password_field.clear()
    password_field.send_keys(WP_PASSWORD)

    # Click login button
    try:
        driver.find_element(By.ID, "wp-submit").click()
    except:
        driver.find_element(By.ID, "cn-accept-cookie").click()
        time.sleep(2)
        driver.find_element(By.ID, "wp-submit").click()

def get_last_order_id_from_sheet():
    """Get the last order ID from the Google Sheet to know where to start"""
    try:
        # Get all records from the sheet
        all_records = sheet.get_all_records()
        
        if not all_records:
            print("No existing records found in sheet. Starting fresh.")
            return None
        
        # The last record is the most recent one
        last_record = all_records[-1]
        last_order_id = last_record['Order ID']
        print(f"Last order ID in sheet: {last_order_id}")
        return last_order_id
        
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return None

def scrape_new_orders_from_page(last_known_order_id):
    """Scrape only new orders from the current page (top to bottom) until we hit last_known_order_id"""
    new_orders_data = []
    
    try:
        table = driver.find_elements(By.CLASS_NAME, "iedit")
        print(f"Found {len(table)} orders on page 1")
        
        # Store main window handle
        main_window = driver.current_window_handles[0]

        # Scrape from top to bottom (newest first) until we find the last known order
        for index in range(len(table)):
            try:
                # Re-find the row in case the table gets stale
                current_table = driver.find_elements(By.CLASS_NAME, "iedit")
                row = current_table[index]
                
                # Extract basic info from the main page first
                order_id = row.find_element(By.CLASS_NAME, "title").text.strip()
                
                # Check if we've reached an order that's already in our sheet
                if last_known_order_id and order_id == last_known_order_id:
                    print(f"Reached last known order ID: {order_id}. Stopping scrape.")
                    break
                
                f_name = row.find_element(By.CLASS_NAME, "wpsc_first_name").text.strip()
                l_name = row.find_element(By.CLASS_NAME, "wpsc_last_name").text.strip()
                email = row.find_element(By.CLASS_NAME, "wpsc_email_address").text.strip()
                payment_status = row.find_element(By.CLASS_NAME, "wpsc_order_status").text.strip()
                date = row.find_element(By.CLASS_NAME, "date").text.strip()
                
                print(f"Processing new order {index + 1}: {order_id}")
                
                # Open order details in new tab
                order_detail_url = f"https://linguistpd.co.uk/wp-admin/post.php?post={order_id}&action=edit"
                
                # Open new tab
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(order_detail_url)
                
                # Get order details quickly
                try:
                    order_element = wait.until(
                        EC.presence_of_element_located((By.NAME, "wpsc_items_ordered"))
                    )
                    total_element = wait.until(
                        EC.presence_of_element_located((By.NAME, "wpsc_total_amount"))
                    )
                    order = order_element.text.strip()
                    amount = total_element.text.strip()
                    print(f"  Order details found: {len(order)} characters \nTotal cost: {amount}")
                except Exception as detail_error:
                    print(f"  Could not find order details: {detail_error}")
                    order = "N/A"
                
                # Close the tab and switch back to main window
                driver.close()
                driver.switch_to.window(main_window)
                
                # Add data to our list (new orders will be appended in chronological order)
                new_orders_data.append([order_id, f_name, l_name, email, amount, payment_status, date, order])
                
                print(f"Completed new order {index + 1}")
                
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                # Ensure we're back on main window if something goes wrong
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.close()
                driver.switch_to.window(main_window)
                continue
                
    except Exception as e:
        print(f"Error finding orders table: {e}")
        print("Please check if you're logged in and on the correct page.")
        print(f"Current URL: {driver.current_url}")
    
    return new_orders_data

def append_to_sheet(new_orders_data):
    """Append new orders to the Google Sheet"""
    if not new_orders_data:
        print("No new orders to append.")
        return
    
    try:
        # Get the next available row
        all_values = sheet.get_all_values()
        next_row = len(all_values) + 1
        
        # Append new data
        sheet.append_rows(new_orders_data)
        print(f"Successfully appended {len(new_orders_data)} new orders to the sheet.")
        
    except Exception as e:
        print(f"Error appending to Google Sheet: {e}")

# ---------------- MAIN EXECUTION ----------------

# First, check the last order ID in the existing sheet
last_known_order_id = get_last_order_id_from_sheet()

# Attempt automatic login
login_to_wordpress()
loggedin = False
while not loggedin:
    try:
        driver.find_element(By.CLASS_NAME, "iedit")
        loggedin = True
    except:
        time.sleep(5)

new_orders_data = []

try:
    # Only scrape Page 1 for new orders
    print("=== CHECKING FOR NEW ORDERS ON PAGE 1 ===")
    driver.get(PAGE_1_URL)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "iedit")))
    
    # Scrape only new orders (from top to bottom until we hit last known order)
    new_orders_data = scrape_new_orders_from_page(last_known_order_id)
    
    if new_orders_data:
        print(f"Found {len(new_orders_data)} new orders to append.")
    else:
        print("No new orders found since last scrape.")

except Exception as e:
    print(f"Error during scraping process: {e}")

finally:
    driver.quit()

# --- Append new orders to Google Sheets ---
if new_orders_data:
    try:
        append_to_sheet(new_orders_data)
        print(f"\n=== UPDATE COMPLETE ===")
        print(f"Successfully added {len(new_orders_data)} new orders to the spreadsheet!")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")
else:
    print("\n=== NO UPDATES NEEDED ===")
    print("No new orders found since last scrape.")