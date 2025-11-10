import time
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

# Load environment variables
load_dotenv()

MAIL_ADR = os.getenv('MAIL_ADR')
PASSWD = os.getenv('PASSWD')

# ---------------- GOOGLE SHEETS SETUP ----------------
def setup_google_sheets():
    """Initialize Google Sheets connection"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", 
                "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "path/to/your/credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open("Email Headers").sheet1
        return sheet
    except Exception as e:
        print(f"Google Sheets setup failed: {e}")
        return None

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
# options.add_argument("--headless")  
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

MAIN_URL = "https://webmail.lcn.com/"

def login_system():
    """Login to the email system with error handling"""
    try:
        print("Navigating to login page...")
        driver.get(MAIN_URL)
        
        # Wait for and fill username
        username_input = wait.until(
            EC.element_to_be_clickable((By.ID, "rcmloginuser"))
        )
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
        print("Login successful!")
        
    except Exception as e:
        print(f"Login failed: {e}")
        driver.save_screenshot("login_error.png")

def extract_email_headers():
    """Extract email headers from the inbox list"""
    try:
        print("Extracting email headers...")
        
        # Wait for emails to load
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )
        
        # Get all email rows
        email_rows = driver.find_elements(By.CSS_SELECTOR, "tr.message")
        print(f"Found {len(email_rows)} emails")
        
        email_data = []
        
        for row in email_rows:
            try:
                # Extract sender
                sender_element = row.find_element(By.CSS_SELECTOR, "span.adr span.rcmContactAddress")
                sender = sender_element.get_attribute("title") or sender_element.text
                
                # Extract subject
                subject_element = row.find_element(By.CSS_SELECTOR, "span.subject a")
                subject = subject_element.text
                
                # Extract date
                date_element = row.find_element(By.CSS_SELECTOR, "span.date")
                date = date_element.text
                
                email_info = {
                    'sender': sender,
                    'subject': subject,
                    'date': date
                }
                
                email_data.append(email_info)
                print(f"✓ {date} - {sender[:30]}... - {subject[:50]}...")
                
            except Exception as e:
                print(f"Error extracting email data: {e}")
                continue
        
        return email_data
        
    except Exception as e:
        print(f"Error getting email list: {e}")
        return []

def save_to_google_sheets(data, sheet):
    """Save email headers to Google Sheets"""
    if not sheet:
        print("Google Sheets not configured - printing data instead")
        for i, email in enumerate(data, 1):
            print(f"{i}. {email['date']} | {email['sender']} | {email['subject']}")
        return
    
    try:
        # Clear existing data (optional)
        sheet.clear()
        
        # Add headers
        sheet.append_row(['Date', 'Sender', 'Subject'])
        
        # Add email data
        for email_data in data:
            row = [
                email_data['date'],
                email_data['sender'],
                email_data['subject']
            ]
            sheet.append_row(row)
        
        print(f"Saved {len(data)} email headers to Google Sheets")
        
    except Exception as e:
        print(f"Error saving to Google Sheets: {e}")

def main():
    """Main execution function"""
    try:
        # Initialize Google Sheets (uncomment when ready)
        # sheet = setup_google_sheets()
        sheet = None
        
        # Login to email
        login_system()
        
        # Wait for inbox to fully load
        time.sleep(2)
        
        # Extract email headers
        email_data = extract_email_headers()
        
        # Save to Google Sheets
        save_to_google_sheets(email_data, sheet)
        
        # Print summary
        print(f"\n=== SUMMARY ===")
        print(f"Total emails extracted: {len(email_data)}")
        print(f"Data saved to Google Sheets: {'Yes' if sheet else 'No (sheet not configured)'}")
        
    except Exception as e:
        print(f"Main execution error: {e}")
        driver.save_screenshot("main_error.png")
        
    finally:
        # Clean up
        time.sleep(2)
        driver.quit()
        print("Script completed")

if __name__ == "__main__":
    main()