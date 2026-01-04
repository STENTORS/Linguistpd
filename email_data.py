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

def setup_google_sheets():
    """Initialize Google Sheets connection"""
    try:  
        SHEET_NAME = "Email LPD data"
        SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

        creds = ServiceAccountCredentials.from_json_keyfile_dict(create_keyfile_dict(), SCOPE)
        client = gspread.authorize(creds)

        sheet = client.open(SHEET_NAME).sheet1
        print("✅ Google Sheets connection established")
        return sheet
    except Exception as e:
        print(f"❌ Google Sheets setup failed: {e}")
        return None

def get_last_sheet_date(sheet):
    """Get the date of the last entry in the sheet"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            print("Sheet is empty or only has headers")
            return None
        
        # Get the last row (excluding header)
        last_row = all_values[-1]
        last_date_str = last_row[0]
        
        if not last_date_str or last_date_str.strip() == "":
            return None
            
        try:
            last_date = datetime.strptime(last_date_str, "%d/%m/%Y %H:%M")
        except ValueError:
            try:
                last_date = datetime.strptime(last_date_str, "%d/%m/%Y")
            except ValueError:
                print(f"Could not parse date format: {last_date_str}")
                return None
        
        print(f"📅 Last date in sheet: {last_date}")
        return last_date
        
    except Exception as e:
        print(f"Error getting last sheet date: {e}")
        return None

def login_system(driver, wait):
    """Login to the email system with error handling"""
    try:
        MAIN_URL = "https://webmail.lcn.com/"
        
        print("🔑 Logging into email system...")
        driver.get(MAIN_URL)
        
        # Wait for and fill username
        username_input = wait.until(
            EC.element_to_be_clickable((By.ID, "rcmloginuser"))
        )
        
        # Get credentials from environment
        MAIL_ADR = os.environ.get('MAIL_ADR')
        PASSWD = os.environ.get('PASSWD')
        
        if not MAIL_ADR or not PASSWD:
            raise ValueError("Email credentials (MAIL_ADR, PASSWD) not found in environment")
        
        username_input.clear()
        username_input.send_keys(MAIL_ADR)
        username_input.send_keys(Keys.ENTER)
        
        # Wait for and fill password
        password_input = wait.until(
            EC.element_to_be_clickable((By.ID, "rcmloginpwd"))
        )
        password_input.clear()
        password_input.send_keys(PASSWD)
        password_input.send_keys(Keys.ENTER)
        
        # Wait for login to complete - wait for inbox to load
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )
        
        print("✅ Login successful")
        return True
        
    except Exception as e:
        print(f"❌ Login failed: {e}")
        try:
            driver.save_screenshot("login_error.png")
            print("Screenshot saved as login_error.png")
        except:
            pass
        return False

def parse_email_date(date_str):
    """Parse email date string to datetime object"""
    try:
        if not date_str:
            return None
            
        # Common date formats in emails
        date_formats = [
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%d %b %Y %H:%M",
            "%d %B %Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%a, %d %b %Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%m/%d/%Y %H:%M",
            "%Y/%m/%d %H:%M:%S"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # If no format matches, try to extract just date part
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y-%m-%d"]:
            try:
                date_part = date_str.split()[0] if ' ' in date_str else date_str
                return datetime.strptime(date_part, fmt)
            except (ValueError, IndexError):
                continue
        
        print(f"⚠️ Could not parse date: {date_str}")
        return None
        
    except Exception as e:
        print(f"Error parsing date {date_str}: {e}")
        return None

def extract_email_headers(driver, wait, last_date=None):
    """Extract email headers from the inbox list, optionally filtering by date"""
    try:
        print("📧 Searching for emails containing 'linguist'...")
        
        # Wait for emails to load
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )

        # Search for "linguist" in emails
        search = wait.until(
            EC.element_to_be_clickable((By.ID, "quicksearchbox"))
        )
        search.clear()
        search.send_keys("linguist")
        search.send_keys(Keys.ENTER)

        # Wait for search results
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )
        
        # Get all email rows
        email_rows = driver.find_elements(By.CSS_SELECTOR, "tr.message")
        print(f"Found {len(email_rows)} emails containing 'linguist'")
        
        email_data = []
        skipped_count = 0
        
        for i, row in enumerate(email_rows):
            try:
                # Extract sender
                sender_element = row.find_element(By.CSS_SELECTOR, "span.adr span.rcmContactAddress")
                sender = sender_element.get_attribute("title") or sender_element.text
                
                # Extract subject
                subject_element = row.find_element(By.CSS_SELECTOR, "span.subject a")
                subject = subject_element.text
                
                # Extract date
                date_element = row.find_element(By.CSS_SELECTOR, "span.date")
                date_str = date_element.text
                
                # Parse the date from the email
                email_date = parse_email_date(date_str)
                
                # Check if this email is newer than the last one in sheet
                if last_date and email_date:
                    if email_date <= last_date:
                        # This email is not newer than what we already have
                        skipped_count += 1
                        continue
                
                email_info = {
                    'sender': sender,
                    'subject': subject,
                    'date_str': date_str,
                    'date_obj': email_date
                }
                
                email_data.append(email_info)
                
                if len(email_data) % 10 == 0:
                    print(f"  Processed {i+1}/{len(email_rows)} emails...")
                
            except Exception as e:
                print(f"  Error processing email row {i}: {e}")
                continue
        
        if skipped_count > 0:
            print(f"  Skipped {skipped_count} emails that were older than last sheet entry")
        
        return email_data
        
    except Exception as e:
        print(f"❌ Error getting email list: {e}")
        return []

def get_all_email_dates(sheet):
    """Get all dates from the sheet to check for duplicates"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return set()
        
        dates = set()
        for i, row in enumerate(all_values[1:]):  # Skip header
            if row and len(row) > 0:  # Check if row is not empty
                date_str = row[0].strip()
                if date_str:  # Only add non-empty dates
                    dates.add(date_str)
        
        print(f"Found {len(dates)} unique dates in existing sheet")
        return dates
        
    except Exception as e:
        print(f"Error getting all dates: {e}")
        return set()

def save_to_google_sheets(data, sheet):
    """Save only new email headers to Google Sheets"""
    try:
        if not data:
            print("No new email data to save")
            return 0
        
        print(f"💾 Saving {len(data)} new emails to Google Sheets...")
        
        # Get existing dates to avoid duplicates
        existing_dates = get_all_email_dates(sheet)
        
        # Filter out any duplicates based on date string
        filtered_data = []
        duplicates_removed = 0
        
        for email in data:
            if email['date_str'] not in existing_dates:
                filtered_data.append(email)
            else:
                duplicates_removed += 1
        
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed} duplicates")
        
        if not filtered_data:
            print("  No new emails after duplicate removal")
            return 0
        
        # Add new email data
        rows_added = 0
        for email_data in filtered_data:
            try:
                row = [
                    email_data['date_str'],
                    email_data['sender'],
                    email_data['subject']
                ]
                sheet.append_row(row)
                rows_added += 1
            except Exception as e:
                print(f"  Error appending row: {e}")
        
        print(f"✅ Appended {rows_added} new email headers to Google Sheets")
        return rows_added
        
    except Exception as e:
        print(f"❌ Error saving to Google Sheets: {e}")
        return 0

def main():
    """Main function to be called from Streamlit app"""
    print("=" * 50)
    print("Starting Email Scraper")
    print("=" * 50)
    
    # Check for required environment variables
    if not os.environ.get("MAIL_ADR") or not os.environ.get("PASSWD"):
        return "Error: Email credentials (MAIL_ADR, PASSWD) not found in environment"
    
    # Initialize components
    driver = None
    try:
        # Setup Selenium
        driver = setup_driver()
        wait = WebDriverWait(driver, 20)
        
        # Initialize Google Sheets
        sheet = setup_google_sheets()
        if not sheet:
            return "Error: Could not initialize Google Sheets connection"
        
        # Get the last date from the sheet
        last_date = get_last_sheet_date(sheet)
        
        # Login to email
        login_success = login_system(driver, wait)
        if not login_success:
            return "Error: Failed to login to email system"
        
        # Wait for inbox to fully load
        time.sleep(2)
        
        # Extract email headers, filtering by last date if available
        email_data = extract_email_headers(driver, wait, last_date)
        
        # Save only new emails to Google Sheets
        rows_added = save_to_google_sheets(email_data, sheet)
        
        # Prepare result message
        if rows_added > 0:
            result_message = f"✅ Successfully added {rows_added} new emails to the spreadsheet!"
        elif email_data and rows_added == 0:
            result_message = "✅ Found emails but they were already in the spreadsheet."
        else:
            result_message = "✅ No new emails found since last scrape. Spreadsheet is up to date."
        
        print("\n" + "=" * 50)
        print(f"SUMMARY: {result_message}")
        print("=" * 50)
        
        return result_message
        
    except Exception as e:
        error_msg = f"❌ Error during email scraping: {str(e)}"
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