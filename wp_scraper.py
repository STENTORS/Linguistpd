import time
import os
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
load_dotenv()

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

# Store the main URL
MAIN_URL = "https://linguistpd.co.uk/wp-admin/edit.php?post_type=wpsc_cart_orders&mode=list"

# ---------------- AUTOMATIC WORDPRESS LOGIN ----------------
def login_to_wordpress():
    """Automatically log in to WordPress"""
    print("Logging in to WordPress...")
    
    # Go to login page
    driver.get(MAIN_URL)
    
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


     
# ---------------- MAIN EXECUTION ----------------
wp_payment_data = [["Order ID", "First Name", "Last Name", "Email", "Total Amount", "Status", "Date", "Order"]]

# Attempt automatic login
login_to_wordpress()
loggedin = False
while not loggedin:
    try:
        driver.find_element(By.CLASS_NAME, "iedit")
        loggedin = True
    except:
        time.sleep(5)

try:
    table = driver.find_elements(By.CLASS_NAME, "iedit")

    sample_size = min(5, len(table))

    # Store main window handle
    main_window = driver.current_window_handle

    for index in range(0,len(table)):
        try:
            # Re-find the row in case the table gets stale
            current_table = driver.find_elements(By.CLASS_NAME, "iedit")
            row = current_table[index]
            
            # Extract basic info from the main page first
            order_id = row.find_element(By.CLASS_NAME, "title").text.strip()
            f_name = row.find_element(By.CLASS_NAME, "wpsc_first_name").text.strip()
            l_name = row.find_element(By.CLASS_NAME, "wpsc_last_name").text.strip()
            email = row.find_element(By.CLASS_NAME, "wpsc_email_address").text.strip()
            amount = row.find_element(By.CLASS_NAME, "wpsc_total_amount").text.strip()
            payment_status = row.find_element(By.CLASS_NAME, "wpsc_order_status").text.strip()
            date = row.find_element(By.CLASS_NAME, "date").text.strip()
            
            print(f"Processing order {index + 1}: {order_id}")
            
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
                order = order_element.text.strip()
                print(f"  Order details found: {len(order)} characters")
            except Exception as detail_error:
                print(f"  Could not find order details: {detail_error}")
                order = "N/A"
            
            # Close the tab and switch back to main window
            driver.close()
            driver.switch_to.window(main_window)
            
            # Add data to our list
            wp_payment_data.append([order_id, f_name, l_name, email, amount, payment_status, date, order])
            
            print(f"Completed order {index + 1}")
            
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

finally:
    driver.quit()

# --- Upload to Google Sheets ---
if len(wp_payment_data) > 1:  # Check if we have data beyond headers
    try:
        sheet.clear()
        sheet.update("A1", wp_payment_data)
        print(f"Successfully processed {len(wp_payment_data)-1} orders and uploaded to Google Sheets!")
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")
else:
    print("No orders were processed.")