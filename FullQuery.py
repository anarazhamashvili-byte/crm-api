import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import time
import datetime

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("your_credentials.json", scope)
client = gspread.authorize(creds)

sheet_orders = client.open_by_key("1nwmKReLDFFtPuGVVUygQrZKXQ5wC2BbGhf4K6EH901k").sheet1
sheet_invoices = client.open_by_key("1qVFFf9f5imgRUqMNV14lnBjlPgB36WJqakEOjAu6a5I").sheet1

# MySQL setup
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Paroli1!",
    database="crmdata"
)
cursor = conn.cursor()

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

def insert_row(row, source):
    try:
        cursor.execute("""
            INSERT INTO orders (
                order_date, order_number, customer_name, phone_number, personal_id,
                product_name, city, branch, location_details, item_carrier, delivery_type,
                order_ready_status, item_collection_note, order_status_1, tracking_code,
                delivery_status_2, standard_deadline, status_update, failed_delivery_comment,
                resend_date, comment_1, issue_date, status, source_sheet
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.get('order_date'), row.get('order_number'), row.get('customer_name'), row.get('phone_number'),
            row.get('personal_id'), row.get('product_name'), row.get('city'), row.get('branch'),
            row.get('location_details'), row.get('item_carrier'), row.get('delivery_type'),
            row.get('order_ready_status'), row.get('item_collection_note'), row.get('order_status_1'),
            row.get('tracking_code'), row.get('delivery_status_2'), row.get('standard_deadline'),
            row.get('status_update'), row.get('failed_delivery_comment'), row.get('resend_date'),
            row.get('comment_1'), row.get('issue_date'), row.get('status'), source
        ))
        return True
    except Exception as e:
        print(f"❌ Error inserting row: {e} | Row preview: {row}")
        return False

def refresh_data():
    inserted = 0
    skipped = 0
    cursor.execute("DELETE FROM orders")

    # Sheet 1: Orders
    headers_orders = [
        'თარიღი', 'ორდერ #', 'მომხმარებელი/კომპანია', 'ტელ. ნომერი', 'პირადი ნომერი/ს.კ',
        'ნომენკლატურა', 'ქალაქი/სოფელი',
        'მისამართი/ფილიალი/საწყობი - ლოკაცია თუ საიდან ხდება ნივთის გაცემა ან სად ხდება ნივთის მიტანა. სწრაფი მიწოდების ის შემთხვევები როდესაც ნივთის უნდა გაიგზავნოს ფილიალიდან, მისამართთან ერთად აუცილებელია მიეთითოს ფილიალი თუ საიდან ხდება სწრაფი  მიწოდების შესრულება',
        'ნივთის გამტანი - აღნიშნული გრაფა ივსება თუ სხვა პიროვნებას გააქვს/იბარებს ნივთს',
        'მიწოდების ტიპი', 'გამზადებულია შეკვეთა',
        'ნივთის მოგროვება - ეს სვეტი ივსება თუ ნივთი ადგილზე არ არის (საწყობი/ფილიალი)',
        'შეკვეთის სტატუსი #1',
        'ALL Tracking Code - ივსება TNT, Quickshipper  & Georgian Post-ის გზავნილის კოდები',
        ' ორდერის მიწოდების სტატუსი #2',
        'სტანდარტული მიწოდების Deadline',
        'Status Update - ინიშნება საწყობიდან გატანის შემთხვევაში ან ნივთის ვერ ჩაბარება/მობრუნების შემთხვევაში',
        'ვერ ჩაბარების კომენტარი',
        'განმეორებითი გაგზავნის თარიღი'
    ]

    try:
        data_orders = sheet_orders.get_all_records(expected_headers=headers_orders)
        for row in data_orders:
            mapped = {
                'order_date': safe_date(row['თარიღი']),
                'order_number': safe_str(row['ორდერ #'], 30),
                'customer_name': safe_str(row['მომხმარებელი/კომპანია'], 150),
                'phone_number': safe_str(row['ტელ. ნომერი'], 50),
                'personal_id': safe_str(row['პირადი ნომერი/ს.კ'], 30),
                'product_name': safe_str(row['ნომენკლატურა'], 255),
                'city': safe_str(row['ქალაქი/სოფელი'], 100),
                'location_details': safe_text(row[headers_orders[7]]),
                'item_carrier': safe_str(row[headers_orders[8]], 100),
                'delivery_type': safe_str(row['მიწოდების ტიპი'], 100),
                'order_ready_status': safe_str(row['გამზადებულია შეკვეთა'], 100),
                'item_collection_note': safe_text(row['ნივთის მოგროვება - ეს სვეტი ივსება თუ ნივთი ადგილზე არ არის (საწყობი/ფილიალი)']),
                'order_status_1': safe_str(row['შეკვეთის სტატუსი #1'], 100),
                'tracking_code': safe_text(row['ALL Tracking Code - ივსება TNT, Quickshipper  & Georgian Post-ის გზავნილის კოდები']),
                'delivery_status_2': safe_str(row[' ორდერის მიწოდების სტატუსი #2'], 100),
                'standard_deadline': safe_date(row['სტანდარტული მიწოდების Deadline']),
                'status_update': safe_text(row['Status Update - ინიშნება საწყობიდან გატანის შემთხვევაში ან ნივთის ვერ ჩაბარება/მობრუნების შემთხვევაში']),
                'failed_delivery_comment': safe_text(row['ვერ ჩაბარების კომენტარი']),
                'resend_date': safe_date(row['განმეორებითი გაგზავნის თარიღი']),
                'source_sheet': 'orders'
            }
            if insert_row(mapped, 'orders'):
                inserted += 1
            else:
                skipped += 1
    except Exception as e:
        print(f"❌ Error reading orders sheet: {e}")

    # Sheet 2: Invoices
    headers_invoices = [
        'თარიღი', 'ინვოისი #', 'მომხმარებელი', 'პირადი ნომერი',
        'ფილიალი', 'პროდუქტი', 'კომენტარი 1',
        'გაცემის თარიღი', 'სტატუსი'
    ]

    try:
        data_invoices = sheet_invoices.get_all_records(expected_headers=headers_invoices)
        for row in data_invoices:
            mapped = {
                'order_date': safe_date(row['თარიღი']),
                'order_number': safe_str(row['ინვოისი #'], 30),
                'customer_name': safe_str(row['მომხმარებელი'], 150),
                'personal_id': safe_str(row['პირადი ნომერი'], 30),
                'branch': safe_str(row['ფილიალი'], 100),
                'product_name': safe_str(row['პროდუქტი'], 255),
                'comment_1': safe_text(row['კომენტარი 1']),
                'issue_date': safe_date(row['გაცემის თარიღი']),
                'status': safe_str(row['სტატუსი'], 100),
                'source_sheet': 'invoices'
            }
            if insert_row(mapped, 'invoices'):
                inserted += 1
            else:
                skipped += 1
    except Exception as e:
        print(f"❌ Error reading invoices sheet: {e}")

    conn.commit()
    print(f"✅ Data refreshed at {datetime.datetime.now().strftime('%H:%M:%S')} | Inserted: {inserted} rows | Skipped: {skipped} rows")

# Refresh every 5 minutes
while True:
    refresh_data()
    time.sleep(600)