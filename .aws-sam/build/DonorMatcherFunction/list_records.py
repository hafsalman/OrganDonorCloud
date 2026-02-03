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
    list_type = event.get("list")

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    if list_type == "donors":
        cursor.execute("""
            SELECT d.donor_id, u.name, u.email, d.age, d.organ, d.blood_group, d.status 
            FROM donors d JOIN users u ON d.user_id = u.user_id
        """)
        data = cursor.fetchall()
        return {"statusCode": 200, "body": json.dumps({"donors": data}, default=str)}

    if list_type == "receivers":
        cursor.execute("""
            SELECT r.receiver_id, u.name, u.email, r.age, r.organ_needed,
                   r.blood_group, r.urgency_level, r.status
            FROM receivers r JOIN users u ON r.user_id = u.user_id
        """)
        data = cursor.fetchall()
        return {"statusCode": 200, "body": json.dumps({"receivers": data}, default=str)}

    return {"statusCode": 400, "body": json.dumps({"error": "Use list=donors or receivers"})}
