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

# Load environment variables
load_dotenv()

MAIL_ADR = os.getenv('MAIL_ADR')
PASSWD = os.getenv('PASSWD')

# ---------------- GOOGLE SHEETS SETUP ----------------
def create_keyfile_dict():
    variables_keys = {
        "type": os.environ.get("SHEET_TYPE"),
        "project_id": os.environ.get("SHEET_PROJECT_ID"),
        "private_key_id": os.environ.get("SHEET_PRIVATE_KEY_ID"),
        "private_key": os.getenv("SHEET_PRIVATE_KEY").replace('\\n', '\n') if os.getenv("SHEET_PRIVATE_KEY") else None,
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
        return sheet
    except Exception as e:
        print(f"Google Sheets setup failed: {e}\n")
        return None

def get_last_sheet_date(sheet):
    """Get the date of the last entry in the sheet"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return None
        # Get the last row (excluding header)
        last_row = all_values[-1]
        last_date_str = last_row[0]
        
        try:
            last_date = datetime.strptime(last_date_str, "%d/%m/%Y %H:%M")
        except ValueError:
            try:
                last_date = datetime.strptime(last_date_str, "%d/%m/%Y")
            except ValueError:
                print(f"Could not parse date: {last_date_str}\n")
                return None
        
        print(f"Last date in sheet: {last_date}\n")
        return last_date
        
    except Exception as e:
        print(f"Error getting last sheet date: {e}\n")
        return None

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
options.add_argument("--headless")  
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

MAIN_URL = "https://webmail.lcn.com/"

def login_system():
    """Login to the email system with error handling"""
    try:
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
        
    except Exception as e:
        print(f"Login failed: {e}\n")
        driver.save_screenshot("login_error.png")

def extract_email_headers(last_date=None):
    """Extract email headers from the inbox list, optionally filtering by date"""
    try:
        
        # Wait for emails to load
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )

        search = wait.until(
            EC.element_to_be_clickable((By.ID, "quicksearchbox"))
        )
        search.clear()
        search.send_keys("linguist")
        search.send_keys(Keys.ENTER)

        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message"))
        )
        
        # Get all email rows
        email_rows = driver.find_elements(By.CSS_SELECTOR, "tr.message")
        print(f"Found {len(email_rows)} emails matching search\n")
        
        email_data = []
        new_emails_found = 0
        
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
                date_str = date_element.text
                
                # Parse the date from the email
                # You may need to adjust this based on the date format in your emails
                try:
                    # Try to parse date (adjust format as needed)
                    email_date = parse_email_date(date_str)
                except Exception as e:
                    print(f"Could not parse email date: {date_str}, error: {e}\n")
                    email_date = None
                
                # Check if this email is newer than the last one in sheet
                if last_date and email_date:
                    if email_date <= last_date:
                        # This email is not newer than what we already have
                        continue
                
                email_info = {
                    'sender': sender,
                    'subject': subject,
                    'date_str': date_str,
                    'date_obj': email_date
                }
                
                email_data.append(email_info)
                new_emails_found += 1
                
            except Exception as e:
                print(f"Error extracting email data: {e}\n")
                continue
        
        print(f"Found {new_emails_found} new emails since last update\n")
        return email_data
        
    except Exception as e:
        print(f"Error getting email list: {e}\n")
        return []

def parse_email_date(date_str):
    """Parse email date string to datetime object"""
    try:
        # Common date formats in emails
        date_formats = [
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%d %b %Y %H:%M",
            "%d %B %Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%a, %d %b %Y %H:%M:%S"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # If no format matches, try to clean and parse
        return None
    except Exception as e:
        print(f"Error parsing date {date_str}: {e}\n")
        return None

def save_to_google_sheets(data, sheet):
    """Save only new email headers to Google Sheets"""
    if not sheet:
        print("Google Sheets not configured - printing data instead\n")
        for i, email in enumerate(data, 1):
            print(f"{i}. {email['date_str']} | {email['sender']} | {email['subject']}")
        return
    
    try:
        # Don't clear existing data - just append new rows
        if len(data) == 0:
            print("No new emails to save\n")
            return
        
        # Add new email data
        for email_data in data:
            row = [
                email_data['date_str'],
                email_data['sender'],
                email_data['subject']
            ]
            sheet.append_row(row)
        
        print(f"Appended {len(data)} new email headers to Google Sheets\n")
        
    except Exception as e:
        print(f"Error saving to Google Sheets: {e}\n")

def get_all_email_dates(sheet):
    """Get all dates from the sheet to check for duplicates"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return set()
        
        dates = set()
        for row in all_values[1:]:  # Skip header
            if row:  # Check if row is not empty
                dates.add(row[0].strip())
        return dates
    except Exception as e:
        print(f"Error getting all dates: {e}\n")
        return set()

def main():
    """Main execution function"""
    try:
        # Initialize Google Sheets
        sheet = setup_google_sheets()
        
        if sheet:
            # Get the last date from the sheet
            last_date = get_last_sheet_date(sheet)
            
            # Get all existing dates to avoid duplicates
            existing_dates = get_all_email_dates(sheet)
            print(f"Found {len(existing_dates)} existing dates in sheet\n")
        else:
            last_date = None
            existing_dates = set()
            print("Sheet not available, will fetch all emails\n")
        
        # Login to email
        login_system()
        
        # Wait for inbox to fully load
        time.sleep(2)
        
        # Extract email headers, filtering by last date if available
        email_data = extract_email_headers(last_date)
        
        # Filter out any duplicates based on date string
        if existing_dates:
            filtered_data = []
            for email in email_data:
                if email['date_str'] not in existing_dates:
                    filtered_data.append(email)
                else:
                    print(f"Duplicate found, skipping: {email['date_str']}\n")
            email_data = filtered_data
        
        # Save only new emails to Google Sheets
        save_to_google_sheets(email_data, sheet)
        
        # Print summary
        print(f"\n=== SUMMARY ===\n")
        print(f"Total new emails found: {len(email_data)}\n")
        print(f"Data saved to Google Sheets: {'Yes' if sheet else 'No (sheet not configured)'}zn")
        
    except Exception as e:
        print(f"Main execution error: {e}\n")
        
    finally:
        # Clean up
        time.sleep(2)
        driver.quit()
        print("\nScript completed")

if __name__ == "__main__":
    main()