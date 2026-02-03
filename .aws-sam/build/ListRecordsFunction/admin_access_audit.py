import json
import mysql.connector
import os

def get_db_connection():
    return mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']

    )

def lambda_handler(event, context):
    user_id = event.get("user_id")

    if not user_id:
        return {"statusCode": 400, "body": json.dumps({"error": "user_id required"})}

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SELECT role FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()

    if not row:
        return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

    if row["role"] != "admin":
        print(f"UNAUTHORIZED ACCESS by {user_id}")
        return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access"})}

    print(f"ADMIN ACCESS by {user_id}")
    return {"statusCode": 200, "body": json.dumps({"message": "Admin access OK"})}
