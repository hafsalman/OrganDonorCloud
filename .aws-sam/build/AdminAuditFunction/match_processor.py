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
    """
    Process match messages from SQS queue.
    This function is triggered by SQS and updates the database.
    """
    processed_records = []
    failed_records = []
    
    for record in event['Records']:
        try:
            # Parse the message body
            match_data = json.loads(record['body'])
            
            donor_id = match_data['donor_id']
            receiver_id = match_data['receiver_id']
            urgency_level = match_data.get('urgency_level', 'low')
            
            # Connect to database
            db = get_db_connection()
            cursor = db.cursor()
            
            try:
                # Start transaction
                db.start_transaction()
                
                # Update donor status
                cursor.execute(
                    "UPDATE donors SET status='matched' WHERE donor_id=%s AND status='pending'",
                    (donor_id,)
                )
                donor_updated = cursor.rowcount
                
                # Update receiver status
                cursor.execute(
                    "UPDATE receivers SET status='matched' WHERE receiver_id=%s AND status='waiting'",
                    (receiver_id,)
                )
                receiver_updated = cursor.rowcount
                
                # Only commit if both updates were successful
                if donor_updated > 0 and receiver_updated > 0:
                    db.commit()
                    processed_records.append({
                        'donor_id': donor_id,
                        'receiver_id': receiver_id,
                        'urgency_level': urgency_level,
                        'status': 'success'
                    })
                    print(f"Successfully processed match: Donor {donor_id} -> Receiver {receiver_id}")
                else:
                    db.rollback()
                    failed_records.append({
                        'donor_id': donor_id,
                        'receiver_id': receiver_id,
                        'reason': 'Record not found or already processed',
                        'status': 'failed'
                    })
                    print(f"Failed to process match: Record not found or already matched")
                    
            except Exception as db_error:
                db.rollback()
                failed_records.append({
                    'donor_id': donor_id,
                    'receiver_id': receiver_id,
                    'reason': str(db_error),
                    'status': 'failed'
                })
                print(f"Database error: {str(db_error)}")
                
            finally:
                cursor.close()
                db.close()
                
        except Exception as e:
            failed_records.append({
                'message_id': record.get('messageId', 'unknown'),
                'reason': str(e),
                'status': 'failed'
            })
            print(f"Error processing record: {str(e)}")
    
    # Return summary
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_successfully': len(processed_records),
            'failed': len(failed_records),
            'total_records': len(event['Records']),
            'processed_records': processed_records,
            'failed_records': failed_records
        })
    }
