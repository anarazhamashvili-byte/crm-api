import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import time
import datetime

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("your_credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("1nwmKReLDFFtPuGVVUygQrZKXQ5wC2BbGhf4K6EH901k").sheet1

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

    inserted = 0
    skipped = 0

    try:
        data = sheet.get_all_records(expected_headers=expected_headers)
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return

    cursor.execute("DELETE FROM orders")

    for row in data:
        try:
            def safe_str(field, max_len):
                try:
                    return str(field).strip()[:max_len]
                except:
                    return ""

            def safe_text(field):
                try:
                    return str(field).strip()
                except:
                    return ""

            def safe_date(field):
                try:
                    return datetime.datetime.strptime(field, "%d/%m/%Y").date()
                except:
                    return None

            order_date = safe_date(row['თარიღი'])
            order_number = safe_str(row['ორდერ #'], 30)
            customer_name = safe_str(row['მომხმარებელი/კომპანია'], 150)
            phone_number = safe_str(row['ტელ. ნომერი'], 50)
            personal_id = safe_str(row['პირადი ნომერი/ს.კ'], 30)
            product_name = safe_str(row['ნომენკლატურა'], 255)
            city = safe_str(row['ქალაქი/სოფელი'], 100)
            location_details = safe_text(row['მისამართი/ფილიალი/საწყობი - ლოკაცია თუ საიდან ხდება ნივთის გაცემა ან სად ხდება ნივთის მიტანა. სწრაფი მიწოდების ის შემთხვევები როდესაც ნივთის უნდა გაიგზავნოს ფილიალიდან, მისამართთან ერთად აუცილებელია მიეთითოს ფილიალი თუ საიდან ხდება სწრაფი  მიწოდების შესრულება'])
            item_carrier = safe_str(row['ნივთის გამტანი - აღნიშნული გრაფა ივსება თუ სხვა პიროვნებას გააქვს/იბარებს ნივთს'], 100)
            delivery_type = safe_str(row['მიწოდების ტიპი'], 100)
            order_ready_status = safe_str(row['გამზადებულია შეკვეთა'], 100)
            item_collection_note = safe_text(row['ნივთის მოგროვება - ეს სვეტი ივსება თუ ნივთი ადგილზე არ არის (საწყობი/ფილიალი)'])
            order_status_1 = safe_str(row['შეკვეთის სტატუსი #1'], 100)
            tracking_code = safe_text(row['ALL Tracking Code - ივსება TNT, Quickshipper  & Georgian Post-ის გზავნილის კოდები'])
            delivery_status_2 = safe_str(row[' ორდერის მიწოდების სტატუსი #2'], 100)
            standard_deadline = safe_date(row['სტანდარტული მიწოდების Deadline'])
            status_update = safe_text(row['Status Update - ინიშნება საწყობიდან გატანის შემთხვევაში ან ნივთის ვერ ჩაბარება/მობრუნების შემთხვევაში'])
            failed_delivery_comment = safe_text(row['ვერ ჩაბარების კომენტარი'])
            resend_date = safe_date(row['განმეორებითი გაგზავნის თარიღი'])

            cursor.execute("""
                INSERT INTO orders (
                    order_date, order_number, customer_name, phone_number, personal_id,
                    product_name, city, location_details, item_carrier, delivery_type,
                    order_ready_status, item_collection_note, order_status_1, tracking_code,
                    delivery_status_2, standard_deadline, status_update, failed_delivery_comment,
                    resend_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_date, order_number, customer_name, phone_number, personal_id,
                product_name, city, location_details, item_carrier, delivery_type,
                order_ready_status, item_collection_note, order_status_1, tracking_code,
                delivery_status_2, standard_deadline, status_update, failed_delivery_comment,
                resend_date
            ))

            inserted += 1

        except Exception as e:
            print(f"❌ Error inserting row: {e} | Row preview: {row}")
            skipped += 1

    conn.commit()
    print(f"✅ Data refreshed at {datetime.datetime.now().strftime('%H:%M:%S')} | Inserted: {inserted} rows | Skipped: {skipped} rows")

# Refresh every 5 minutes
while True:
    refresh_data()
    time.sleep(300)