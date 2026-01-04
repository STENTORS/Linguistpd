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
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """Setup Selenium WebDriver with Chrome"""
    try:
        # Configure Chrome options for cloud environments
        chrome_options = Options()
        
        # Essential for cloud/headless environments
        chrome_options.add_argument("--headless")  # Run without GUI
        chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
        chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid /dev/shm issues
        chrome_options.add_argument("--disable-gpu")  # GPU not available in cloud
        chrome_options.add_argument("--window-size=1920,1080")
        
        # For newer Chrome versions
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Cloud environments often have Chrome pre-installed at specific locations
        chrome_options.binary_location = os.getenv("GOOGLE_CHROME_BIN", "/usr/bin/google-chrome")
        
        # Use webdriver-manager to automatically manage ChromeDriver
        # This handles downloading the correct version automatically
        service = Service(ChromeDriverManager().install())
        
        # Create driver with explicit service
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
        
    except Exception as e:
        print(f"Error setting up Chrome driver: {e}")
        # Fallback to traditional method
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=chrome_options)
        return driver

def create_keyfile_dict():
    """Create Google Sheets credentials dictionary from environment variables"""
    variables_keys = {
        "type": os.environ.get("SHEET_TYPE"),
        "project_id": os.environ.get("SHEET_PROJECT_ID"),
        "private_key_id": os.environ.get("SHEET_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("SHEET_PRIVATE_KEY", "").replace('\\n', '\n'),
        "client_email": os.environ.get("SHEET_CLIENT_EMAIL"),
        "client_id": os.environ.get("SHEET_CLIENT_ID"),
        "auth_uri": os.environ.get("SHEET_AUTH_URI"),
        "token_uri": os.environ.get("SHEET_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.environ.get("SHEET_AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.environ.get("SHEET_CLIENT_X509_CERT_URL"),
    }
    return variables_keys

def initialize_google_sheets():
    """Initialize Google Sheets connection"""
    SHEET_NAME = "WordPress Sales Data"
    SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).sheet1
        return sheet
    except Exception as e:
        print(f"Error initializing Google Sheets: {e}")
        return None

def login_to_wordpress(driver, wait):
    """Automatically log in to WordPress"""
    print("Logging in to WordPress...")
    
    # Store the specific page URL for latest orders
    PAGE_1_URL = "https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list&paged=1"
    
    # Go to login page using page 1 URL
    driver.get(PAGE_1_URL)
    
    # Wait for login form to load
    wait.until(EC.presence_of_element_located((By.ID, "user_login")))
    
    # Get credentials from environment
    WP_USERNAME = os.environ.get("WP_USERNAME")
    WP_PASSWORD = os.environ.get("WP_PASSWORD")
    
    if not WP_USERNAME or not WP_PASSWORD:
        raise ValueError("WordPress credentials not found in environment variables")
    
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
        # Handle cookie consent if present
        try:
            driver.find_element(By.ID, "cn-accept-cookie").click()
            time.sleep(2)
            driver.find_element(By.ID, "wp-submit").click()
        except:
            driver.find_element(By.ID, "wp-submit").click()

def get_last_order_id_from_sheet(sheet):
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

def scrape_new_orders_from_page(driver, wait, last_known_order_id):
    """Scrape only new orders from the current page (top to bottom) until we hit last_known_order_id"""
    new_orders_data = []
    PAGE_1_URL = "https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list&paged=1"
    
    try:
        driver.get(PAGE_1_URL)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "iedit")))
        
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
                    amount = "N/A"
                
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

def append_to_sheet(sheet, new_orders_data):
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

def main():
    """Main function to be called from Streamlit app"""
    print("Starting WordPress scraper...")
    
    # Check for required environment variables
    if not os.environ.get("WP_USERNAME") or not os.environ.get("WP_PASSWORD"):
        return "Error: WordPress credentials (WP_USERNAME, WP_PASSWORD) not found in environment"
    
    # Initialize components
    driver = None
    try:
        # Setup Selenium
        driver = setup_driver()
        wait = WebDriverWait(driver, 20)
        
        # Initialize Google Sheets
        sheet = initialize_google_sheets()
        if not sheet:
            return "Error: Could not initialize Google Sheets connection"
        
        # Check the last order ID in the existing sheet
        last_known_order_id = get_last_order_id_from_sheet(sheet)
        
        # Login to WordPress
        login_to_wordpress(driver, wait)
        
        # Verify login
        loggedin = False
        login_attempts = 0
        while not loggedin and login_attempts < 5:
            try:
                driver.find_element(By.CLASS_NAME, "iedit")
                loggedin = True
            except:
                time.sleep(5)
                login_attempts += 1
                print(f"Login verification attempt {login_attempts}/5")
        
        if not loggedin:
            return "Error: Could not verify WordPress login"
        
        # Scrape new orders
        new_orders_data = []
        print("=== CHECKING FOR NEW ORDERS ON PAGE 1 ===")
        
        new_orders_data = scrape_new_orders_from_page(driver, wait, last_known_order_id)
        
        if new_orders_data:
            print(f"Found {len(new_orders_data)} new orders to append.")
        else:
            print("No new orders found since last scrape.")
        
        # Append to Google Sheets
        if new_orders_data:
            append_to_sheet(sheet, new_orders_data)
            result_message = f"✅ Successfully added {len(new_orders_data)} new orders to the spreadsheet!"
        else:
            result_message = "✅ No new orders found since last scrape. Spreadsheet is up to date."
        
        return result_message
        
    except Exception as e:
        error_msg = f"❌ Error during WordPress scraping: {str(e)}"
        print(error_msg)
        return error_msg
        
    finally:
        # Ensure driver is closed
        if driver:
            try:
                driver.quit()
                print("Chrome driver closed.")
            except:
                pass

# Keep this for backward compatibility if running as a standalone script
if __name__ == "__main__":
    result = main()
    print(result)
    import sys
    sys.exit(0 if "✅" in result or "Successfully" in result else 1)