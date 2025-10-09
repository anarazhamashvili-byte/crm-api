import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import time
import datetime

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("your_credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("1qVFFf9f5imgRUqMNV14lnBjlPgB36WJqakEOjAu6a5I").sheet1

# MySQL setup
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Paroli1!",
    database="crmdata"
)
cursor = conn.cursor()

def refresh_data():
    expected_headers = [
    'თარიღი', 'ინვოისი #', 'მომხმარებელი', 'პირადი ნომერი',
    'ფილიალი', 'პროდუქტი', 'კომენტარი 1',
    'გაცემის თარიღი', 'სტატუსი'
]

    inserted = 0
    skipped = 0

    try:
        data = sheet.get_all_records(expected_headers=expected_headers)
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return

    cursor.execute("DELETE FROM invoices")

    for row in data:
        try:
            def safe_str(field, max_len):
                try:
                    return str(field).strip()[:max_len]
                except:
                    return ""

            def safe_text(field, max_len=None):
                try:
                    text = str(field).strip()
                    return text[:max_len] if max_len else text
                except:
                    return ""

            def safe_date(field):
                try:
                    return datetime.datetime.strptime(field, "%d/%m/%Y").date()
                except:
                    return None

            order_date = safe_date(row['თარიღი'])
            order_number = safe_str(row['ინვოისი #'], 30)
            customer_name = safe_str(row['მომხმარებელი'], 150)
            personal_id = safe_str(row['პირადი ნომერი'], 30)
            branch = safe_str(row['ფილიალი'], 100)
            product_name = safe_str(row['პროდუქტი'], 255)
            comment_1 = safe_text(row['კომენტარი 1'])
            issue_date = safe_date(row['გაცემის თარიღი'])
            status = safe_str(row['სტატუსი'], 100)

            cursor.execute("""
                INSERT INTO invoices (
                    order_date, order_number, customer_name, personal_id,
                    branch, product_name, comment_1, issue_date, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_date, order_number, customer_name, personal_id,
                branch, product_name, comment_1, issue_date, status
            ))

            inserted += 1

        except Exception as e:
            print(f"❌ Error inserting row: {e} | Row preview: {row}")
            skipped += 1

    conn.commit()
    print(f"✅ Invoice data refreshed at {datetime.datetime.now().strftime('%H:%M:%S')} | Inserted: {inserted} rows | Skipped: {skipped} rows")

# Refresh every 5 minutes
while True:
    refresh_data()
    time.sleep(300)