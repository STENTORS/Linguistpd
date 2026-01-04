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
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """Setup Selenium WebDriver with Chrome"""
    try:
        # Configure Chrome options for cloud environments
        chrome_options = Options()
        
        # Essential for cloud/headless environments
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Use webdriver-manager for automatic ChromeDriver management
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
    private_key = os.environ.get("SHEET_PRIVATE_KEY", "")
    if private_key:
        private_key = private_key.replace('\\n', '\n')
    
    variables_keys = {
        "type": os.environ.get("SHEET_TYPE"),
        "project_id": os.environ.get("SHEET_PROJECT_ID"),
        "private_key_id": os.environ.get("SHEET_PRIVATE_KEY_ID"),
        "private_key": private_key,
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
    try:
        SHEET_NAME = "LinguistPd Buffer Data"
        SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).sheet1
        print("✅ Google Sheets connection established")
        return sheet
    except Exception as e:
        print(f"❌ Google Sheets setup failed: {e}")
        return None

def parse_date(date_str):
    """
    Convert dates like "Friday, 18 July" or "Monday, 30 December 2024" to dd/mm/yyyy
    Assumes current year if year not specified
    """
    try:
        # Remove the day of week prefix (e.g., "Friday, ")
        date_part = date_str.split(', ', 1)[1]
        
        # Try parsing with year first
        try:
            dt = datetime.strptime(date_part, "%d %B %Y")
        except ValueError:
            # If no year, assume current year
            dt = datetime.strptime(date_part, "%d %B")
            current_year = datetime.now().year
            dt = dt.replace(year=current_year)
        
        # Format as dd/mm/yyyy
        return dt.strftime("%d/%m/%Y")
    except Exception as e:
        print(f"⚠️ Could not parse date '{date_str}': {e}")
        return date_str

def get_last_date_from_sheet(sheet):
    """Get the last date from the Google Sheet"""
    try:
        all_rows = sheet.get_all_values()
        if len(all_rows) <= 1:  # Only header or empty
            print("Sheet is empty or only has headers")
            return "01/01/2000"  # Return very old date to get all posts
        
        last_row = all_rows[-1]
        if not last_row or len(last_row) == 0:
            return "01/01/2000"
            
        last_date = last_row[0]
        return parse_date(last_date)
    except Exception as e:
        print(f"Error getting last date from sheet: {e}")
        return "01/01/2000"

def login_to_buffer(driver, wait):
    """Login to Buffer with error handling"""
    try:
        # Get credentials from environment
        buffer_user = os.environ.get("buffer_user")
        buffer_pass = os.environ.get("buffer_pass")
        
        if not buffer_user or not buffer_pass:
            raise ValueError("Buffer credentials (buffer_user, buffer_pass) not found in environment")
        
        print("🔑 Logging into Buffer...")
        
        # Navigate to Buffer
        driver.get("https://publish.buffer.com/all-channels?tab=sent")
        
        # Wait for page to load
        time.sleep(3)
        
        # Try to reject cookies if present
        try:
            reject_cookies = wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "cky-btn-reject"))
            )
            reject_cookies.click()
            time.sleep(1)
        except:
            pass  # Cookie banner might not be present
        
        # Find and fill login form
        username_field = wait.until(
            EC.element_to_be_clickable((By.NAME, "email"))
        )
        password_field = driver.find_element(By.NAME, "password")
        
        # Clear fields and enter credentials
        username_field.clear()
        username_field.send_keys(buffer_user)
        
        password_field.clear()
        password_field.send_keys(buffer_pass)
        
        # Click login button
        enter_login = driver.find_element(By.ID, "login-form-submit")
        enter_login.click()
        
        # Wait for login to complete
        wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "publish_timeline_qL9zu"))
        )
        
        print("✅ Login successful")
        return True
        
    except Exception as e:
        print(f"❌ Login failed: {e}")
        try:
            driver.save_screenshot("buffer_login_error.png")
            print("Screenshot saved as buffer_login_error.png")
        except:
            pass
        return False

def scrape_buffer_posts(driver, wait, last_sheet_date):
    """Scrape posts from Buffer with infinite scroll"""
    print("📱 Scraping Buffer posts...")
    
    try:
        # Store main window handle
        main_window = driver.current_window_handles[0]
        
        # Infinite scroll to load all posts
        print("  Scrolling to load all posts...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Wait for content to load
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                scroll_attempts += 1
                if scroll_attempts >= 3:  # If no change for 3 attempts, break
                    break
            else:
                scroll_attempts = 0  # Reset if we got new content
                
            last_height = new_height
        
        print(f"  Finished scrolling. Document height: {last_height}")
        
        # Find the timeline wrapper
        wrapper = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "publish_timeline_qL9zu"))
        )
        elements = wrapper.find_elements(By.XPATH, "./*")
        
        print(f"  Found {len(elements)} timeline elements")
        
        data = []
        current_date = None
        new_posts_count = 0
        skipped_posts_count = 0
        
        for i, block in enumerate(elements):
            try:
                class_name = block.get_attribute("class")
                
                # Date header
                if "publish_base_Y1USt" in class_name:
                    current_date = block.text.strip()
                    parsed_current_date = parse_date(current_date)
                    
                    # Check if this date is newer than our last sheet date
                    if parsed_current_date <= last_sheet_date:
                        # Skip posts from this date and older dates
                        continue
                
                # Post container
                elif "publish_postContainer" in class_name or "publish_wrapper_KDBT-" in class_name:
                    # Skip if we don't have a current date or if date is too old
                    if not current_date or parse_date(current_date) <= last_sheet_date:
                        skipped_posts_count += 1
                        continue
                    
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
                        "Likes": "0",
                        "Reactions": "0",
                        "Comments": "0",
                        "Impressions": "0",
                        "Shares": "0",
                        "Clicks": "0",
                        "Eng. Rate": "0"
                    }
                    
                    # Extract metrics
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
                    
                    # Calculate values for sheet
                    # Merge Likes/Reactions
                    try:
                        likes_reactions = int(metrics.get("Likes", "0") or 0) + int(metrics.get("Reactions", "0") or 0)
                    except:
                        likes_reactions = 0
                    
                    # Clicks or Engagement Rate
                    try:
                        clicks_eng = int(metrics.get("Clicks", "0") or 0) + int(metrics.get("Eng. Rate", "0") or 0)
                    except:
                        clicks_eng = 0
                    
                    # Comments
                    try:
                        comment_val = int(metrics.get("Comments", "0") or 0)
                    except:
                        comment_val = 0
                    
                    # Impressions
                    try:
                        immpression_val = int(metrics.get("Impressions", "0") or 0)
                    except:
                        immpression_val = 0
                    
                    # Shares
                    try:
                        share_val = int(metrics.get("Shares", "0") or 0)
                    except:
                        share_val = 0
                    
                    # Calculate social score
                    social_score = likes_reactions + clicks_eng + comment_val + immpression_val + share_val
                    
                    # Append data
                    data.append([
                        parse_date(current_date),
                        time_text,
                        platform_info,
                        post_text[:500],  # Limit post text length
                        str(likes_reactions),
                        metrics.get("Comments", "0"),
                        metrics.get("Impressions", "0"),
                        metrics.get("Shares", "0"),
                        str(clicks_eng),
                        str(social_score)
                    ])
                    
                    new_posts_count += 1
                    
                    if new_posts_count % 10 == 0:
                        print(f"  Processed {new_posts_count} new posts...")
                        
            except Exception as e:
                print(f"  Error processing element {i}: {e}")
                continue
        
        print(f"  Found {new_posts_count} new posts, skipped {skipped_posts_count} old posts")
        return data
        
    except Exception as e:
        print(f"❌ Error scraping Buffer posts: {e}")
        return []

def upload_to_google_sheets(sheet, data):
    """Upload data to Google Sheets"""
    if not data:
        print("No new data to upload")
        return 0
    
    try:
        print(f"💾 Uploading {len(data)} posts to Google Sheets...")
        
        rows_added = 0
        for i, row in enumerate(data):
            try:
                sheet.append_row(row)
                rows_added += 1
                
                if rows_added % 10 == 0:
                    print(f"  Uploaded {rows_added}/{len(data)} posts...")
                    
            except Exception as e:
                print(f"  Error uploading row {i}: {e}")
        
        print(f"✅ Successfully uploaded {rows_added} posts to Google Sheets")
        return rows_added
        
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")
        return 0

def main():
    """Main function to be called from Streamlit app"""
    print("=" * 50)
    print("Starting Buffer Social Media Scraper")
    print("=" * 50)
    
    # Check for required environment variables
    if not os.environ.get("buffer_user") or not os.environ.get("buffer_pass"):
        return "Error: Buffer credentials (buffer_user, buffer_pass) not found in environment"
    
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
        
        # Get the last date from the sheet
        last_sheet_date = get_last_date_from_sheet(sheet)
        print(f"📅 Last date in sheet: {last_sheet_date}")
        
        # Login to Buffer
        login_success = login_to_buffer(driver, wait)
        if not login_success:
            return "Error: Failed to login to Buffer"
        
        # Scrape posts
        data = scrape_buffer_posts(driver, wait, last_sheet_date)
        
        # Upload to Google Sheets
        rows_added = upload_to_google_sheets(sheet, data)
        
        # Prepare result message
        if rows_added > 0:
            result_message = f"✅ Successfully added {rows_added} new social media posts to the spreadsheet!"
        else:
            result_message = "✅ No new social media posts found since last scrape. Spreadsheet is up to date."
        
        print("\n" + "=" * 50)
        print(f"SUMMARY: {result_message}")
        print("=" * 50)
        
        return result_message
        
    except Exception as e:
        error_msg = f"❌ Error during Buffer scraping: {str(e)}"
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