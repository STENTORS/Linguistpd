import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- GOOGLE SHEETS SETUP ----------------

#https://docs.google.com/spreadsheets/d/1bJ9GHCusbuh2eI45kSoExNimAqifIJG8yU8bAz5T7XU/edit?usp=sharing 

MAIN_URL = "https://webmail.lcn.com/7TkF8tJUBEU7ASSN/?_task=mail&_mbox=INBOX"

#collect the date, author and title for each email by the news letters