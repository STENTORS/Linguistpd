import time
import re
from datetime import datetime, timedelta
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

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

MAIL_ADR = os.getenv('MAIL_ADR')
PASSWD = os.getenv('PASSWD')

# ---------------- GOOGLE SHEETS SETUP ----------------
def setup_google_sheets():
    try:
        SHEET_NAME = "Email LPD data"
        SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).sheet1
        return sheet
    except Exception as e:
        print(f"Google Sheets setup failed: {e}")
        return None


def parse_email_date(date_str):
    """
    Parse all date formats from the webmail inbox:
      - "Thu 12:31"        -> most recent Thursday at that time
      - "Mon 07:16"        -> most recent Monday at that time
      - "09/02/2026 07:16" -> dd/mm/yyyy HH:MM
      - "19/01/2026 12:16" -> dd/mm/yyyy HH:MM
    Returns a datetime or None.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    s = date_str.strip()
    today = datetime.now()

    # "dd/mm/yyyy HH:MM" or "dd/mm/yyyy"
    m = re.match(r'^(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}))?', s)
    if m:
        try:
            dt_str = m.group(1) + (" " + m.group(2) if m.group(2) else "")
            fmt = "%d/%m/%Y %H:%M" if m.group(2) else "%d/%m/%Y"
            return datetime.strptime(dt_str, fmt)
        except Exception:
            pass

    # "DayAbbr HH:MM" e.g. "Thu 12:31"
    days_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
    m = re.match(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d{1,2}:\d{2})$', s)
    if m:
        try:
            target_weekday = days_map[m.group(1)]
            days_ago = (today.weekday() - target_weekday) % 7
            email_date = today - timedelta(days=days_ago)
            hour, minute = map(int, m.group(2).split(':'))
            email_date = email_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            # Same weekday but time in the future means it was last week
            if days_ago == 0 and email_date > today:
                email_date -= timedelta(weeks=1)
            return email_date
        except Exception:
            pass

    # Fallback: dateutil
    try:
        from dateutil import parser as du_parser
        return du_parser.parse(s, dayfirst=True)
    except Exception:
        pass

    print(f"  [WARN] Could not parse email date: '{date_str}'")
    return None


def get_latest_date_from_sheet(sheet):
    """Return the most recent datetime in the sheet, or None if empty."""
    try:
        all_records = sheet.get_all_records()
        if not all_records:
            print("Sheet is empty. Will scrape all emails.")
            return None

        # Sheet is newest-first; row 0 after header is most recent
        raw = str(all_records[0].get('Date', '')).strip()
        latest = parse_email_date(raw)
        print(f"Latest date in sheet: {raw} -> {latest}")
        return latest
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
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
    try:
        print("Navigating to login page...")
        driver.get(MAIN_URL)

        username_input = wait.until(EC.element_to_be_clickable((By.ID, "rcmloginuser")))
        username_input.clear()
        username_input.send_keys(MAIL_ADR)
        username_input.send_keys(Keys.ENTER)

        password_input = wait.until(EC.element_to_be_clickable((By.ID, "rcmloginpwd")))
        password_input.clear()
        password_input.send_keys(PASSWD)
        password_input.send_keys(Keys.ENTER)

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message")))
        print("Login successful!")
    except Exception as e:
        print(f"Login failed: {e}")
        driver.save_screenshot("login_error.png")


def extract_new_email_headers(latest_known_date):
    """
    Extract email headers from the inbox, stopping at emails already in the sheet.
    Inbox is newest-first, so we stop the moment we hit the cutoff.
    """
    try:
        print("Extracting email headers...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message")))

        search = wait.until(EC.element_to_be_clickable((By.ID, "quicksearchbox")))
        search.clear()
        search.send_keys("linguist")
        search.send_keys(Keys.ENTER)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr.message")))

        email_rows = driver.find_elements(By.CSS_SELECTOR, "tr.message")
        print(f"Found {len(email_rows)} matching emails")

        new_emails = []

        for row in email_rows:
            try:
                sender_element = row.find_element(By.CSS_SELECTOR, "span.adr span.rcmContactAddress")
                sender = sender_element.get_attribute("title") or sender_element.text

                subject_element = row.find_element(By.CSS_SELECTOR, "span.subject a")
                subject = subject_element.text

                date_element = row.find_element(By.CSS_SELECTOR, "span.date")
                date_raw = date_element.text

                parsed_date = parse_email_date(date_raw)

                # Stop once we reach emails at or before what's already stored
                if latest_known_date and parsed_date and parsed_date <= latest_known_date:
                    print(f"  Reached cutoff ({date_raw}). Stopping.")
                    break

                new_emails.append({'date': date_raw, 'sender': sender, 'subject': subject})
                print(f"  + {date_raw} | {sender[:30]} | {subject[:50]}")

            except Exception as e:
                print(f"  Row error: {e}")
                continue

        return new_emails

    except Exception as e:
        print(f"Error getting email list: {e}")
        return []


def save_to_google_sheets(data, sheet, latest_known_date):
    """Fresh run: clear + write header + all data. Incremental: prepend after header."""
    if not sheet:
        print("Google Sheets not configured")
        for i, e in enumerate(data, 1):
            print(f"{i}. {e['date']} | {e['sender']} | {e['subject']}")
        return

    try:
        rows = [[e['date'], e['sender'], e['subject']] for e in data]

        if latest_known_date is None:
            sheet.clear()
            sheet.append_row(['Date', 'Sender', 'Subject'])
            if rows:
                sheet.insert_rows(rows, row=2)
            print(f"Fresh upload: {len(rows)} emails written.")
        else:
            if rows:
                sheet.insert_rows(rows, row=2)
            print(f"Prepended {len(rows)} new email(s).")

    except Exception as e:
        print(f"Error saving to Google Sheets: {e}")


def main():
    try:
        sheet = setup_google_sheets()
        latest_known_date = get_latest_date_from_sheet(sheet) if sheet else None

        login_system()
        time.sleep(2)

        new_emails = extract_new_email_headers(latest_known_date)

        print(f"\n=== SUMMARY ===")
        print(f"New emails found: {len(new_emails)}")

        if new_emails:
            save_to_google_sheets(new_emails, sheet, latest_known_date)
        else:
            print("No new emails since last run. Sheet unchanged.")

    except Exception as e:
        print(f"Main execution error: {e}")
        driver.save_screenshot("main_error.png")
    finally:
        time.sleep(2)
        driver.quit()
        print("Script completed")


if __name__ == "__main__":
    main()