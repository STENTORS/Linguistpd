import requests
from requests.auth import HTTPDigestAuth

URL = "https://www.linkedin.com/feed/"

requests.get(url, auth=HTTPDigestAuth('user', 'pass'))

page = requests.get(URL)

print(page.text)