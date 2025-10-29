import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------

#https://docs.google.com/spreadsheets/d/1bJ9GHCusbuh2eI45kSoExNimAqifIJG8yU8bAz5T7XU/edit?usp=sharing 

# ---------------- SELENIUM SETUP ----------------
options = webdriver.FirefoxOptions()
#options.add_argument("--headless")  
driver = webdriver.Firefox(options=options)
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

MAIN_URL = "https://mail.google.com/mail/u/5/#inbox"

#collect the date, author and title for each email by the news letters
def login_system():
    driver.get(MAIN_URL)
login_system()