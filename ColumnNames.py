import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("your_credentials.json", scope)
client = gspread.authorize(creds)

# Open the sheet
sheet = client.open_by_key("1nwmKReLDFFtPuGVVUygQrZKXQ5wC2BbGhf4K6EH901k").sheet1

# Get and print the first row (headers)
headers = sheet.row_values(1)
print("🧾 Column headers:")
for i, header in enumerate(headers, start=1):
    print(f"{i}. {header}")