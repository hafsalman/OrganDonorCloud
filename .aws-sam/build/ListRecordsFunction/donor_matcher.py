import json
import mysql.connector
import boto3
import os
from datetime import datetime

# Initialize AWS clients
sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')

def get_db_connection():
    return mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )

def lambda_handler(event, context):
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    # Fetch pending donors
    cursor.execute("SELECT * FROM donors WHERE status='pending'")
    donors = cursor.fetchall()
    
    # Fetch waiting receivers with urgency level for priority sorting
    cursor.execute("SELECT * FROM receivers WHERE status='waiting' ORDER BY FIELD(urgency_level, 'high', 'medium', 'low')")
    receivers = cursor.fetchall()
    
    matches = []
    
    # Priority mapping for urgency levels
    urgency_priority = {'high': 1, 'medium': 2, 'low': 3}
    
    # Find matches
    for d in donors:
        for r in receivers:
            if d["organ"] == r["organ_needed"] and d["blood_group"] == r["blood_group"]:
                match_data = {
                    "donor_id": d["donor_id"],
                    "receiver_id": r["receiver_id"],
                    "organ": d["organ"],
                    "blood_group": d["blood_group"],
                    "urgency_level": r.get("urgency_level", "low"),
                    "receiver_name": r.get("name", "Unknown"),
                    "donor_name": d.get("name", "Unknown"),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                matches.append(match_data)
                
                # Determine which queue to use based on urgency
                queue_url = os.environ.get('PRIORITY_QUEUE_URL') if r.get("urgency_level") == 'high' else os.environ.get('STANDARD_QUEUE_URL')
                
                try:
                    # Send match to SQS with priority
                    sqs_response = sqs_client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(match_data),
                        MessageAttributes={
                            'urgency_level': {
                                'DataType': 'String',
                                'StringValue': r.get("urgency_level", "low")
                            },
                            'organ': {
                                'DataType': 'String',
                                'StringValue': d["organ"]
                            }
                        }
                    )
                    
                    # Send SNS notification
                    sns_message = f"""
🏥 ORGAN MATCH FOUND! 🏥

Match Details:
- Organ: {d["organ"]}
- Blood Group: {d["blood_group"]}
- Urgency Level: {r.get("urgency_level", "low").upper()}
- Donor ID: {d["donor_id"]}
- Receiver ID: {r["receiver_id"]}
- Timestamp: {match_data["timestamp"]}

This match has been queued for processing.
Priority: {urgency_priority.get(r.get("urgency_level", "low"), 3)}
                    """
                    
                    sns_client.publish(
                        TopicArn=os.environ['SNS_TOPIC_ARN'],
                        Subject=f'🚨 URGENT: Organ Match Found - {r.get("urgency_level", "low").upper()} Priority',
                        Message=sns_message.strip()
                    )
                    
                    print(f"Match sent to queue: {sqs_response['MessageId']}")
                    
                except Exception as e:
                    print(f"Error sending match to queue/SNS: {str(e)}")
                    # Continue processing other matches even if one fails
                
                # Break to avoid matching one donor to multiple receivers
                break
    
    cursor.close()
    db.close()
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Matching process completed",
            "matches_found": len(matches),
            "matches_queued": len(matches),
            "priority_breakdown": {
                "high": len([m for m in matches if m["urgency_level"] == "high"]),
                "medium": len([m for m in matches if m["urgency_level"] == "medium"]),
                "low": len([m for m in matches if m["urgency_level"] == "low"])
            }
        })
    }
