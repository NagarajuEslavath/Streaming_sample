import boto3
import time
from botocore.exceptions import ClientError
 
# Configuration
LOG_GROUP_NAME = "MyApplicationLogs"
LOG_STREAM_NAME = "MyAppStream"
 
# Create a CloudWatch Logs client
client = boto3.client("logs", region_name="ap-south-1")
 
 
def create_log_group():
    """Create log group if it doesn't exist"""
    try:
        client.create_log_group(logGroupName=LOG_GROUP_NAME)
        print(f"Log group '{LOG_GROUP_NAME}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"Log group '{LOG_GROUP_NAME}' already exists.")
        else:
            raise
 
 
def create_log_stream():
    """Create log stream if it doesn't exist"""
    try:
        client.create_log_stream(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=LOG_STREAM_NAME
        )
        print(f"Log stream '{LOG_STREAM_NAME}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"Log stream '{LOG_STREAM_NAME}' already exists.")
        else:
            raise
 
 
def get_sequence_token():
    """Retrieve the current sequence token for the log stream"""
    try:
        response = client.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            logStreamNamePrefix=LOG_STREAM_NAME
        )
 
        streams = response.get("logStreams", [])
        if streams and "uploadSequenceToken" in streams[0]:
            return streams[0]["uploadSequenceToken"]
 
        return None
    except ClientError as e:
        print("Error getting sequence token:", e)
        return None
 
 
def send_log(message):
    """Send a log message to CloudWatch"""
    timestamp = int(time.time() * 1000)  # milliseconds
    sequence_token = get_sequence_token()
 
    log_event = {
        "logGroupName": LOG_GROUP_NAME,
        "logStreamName": LOG_STREAM_NAME,
        "logEvents": [
            {
                "timestamp": timestamp,
                "message": message
            }
        ]
    }
 
    # Include sequence token if available
    if sequence_token:
        log_event["sequenceToken"] = sequence_token
 
    try:
        response = client.put_log_events(**log_event)
        print("Log sent successfully!")
 
        return response.get("nextSequenceToken")
 
    except ClientError as e:
        # Handle invalid sequence token (common issue)
        if e.response["Error"]["Code"] == "InvalidSequenceTokenException":
            print("Sequence token invalid, retrying...")
 
            # Get correct token and retry
            correct_token = e.response["Error"]["Message"].split(" ")[-1]
            log_event["sequenceToken"] = correct_token
 
            response = client.put_log_events(**log_event)
            print("Log sent successfully after retry!")
 
            return response.get("nextSequenceToken")
 
        else:
            print("Failed to send log:", e)
 
 
def main():
    """Main function"""
    create_log_group()
    create_log_stream()
 
    # Example logs
    send_log("Application started")
    send_log("Processing data...")
    send_log("Application finished successfully")
 
 
if __name__ == "__main__":
    main()