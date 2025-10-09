from flask import Flask, jsonify
import mysql.connector
import os

app = Flask(__name__)

# Load MySQL credentials from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Paroli1!")
DB_NAME = os.getenv("DB_NAME", "crmdata")

def get_orders():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 100")
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        return {"error": str(e)}

@app.route("/api/orders", methods=["GET"])
def orders_endpoint():
    data = get_orders()
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)