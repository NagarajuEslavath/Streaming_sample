import boto3
import time

def put_cloudwatch_log(log_group_name, log_stream_name, log_message):
    """
    Creates log groups/streams if missing, and publishes a log event to AWS CloudWatch.
    """
    # Initialize the CloudWatch Logs client
    client = boto3.client('logs')
    
    # 1. Ensure the Log Group exists
    try:
        client.create_log_group(logGroupName=log_group_name)
        print(f"Created Log Group: {log_group_name}")
    except client.exceptions.ResourceAlreadyExistsException:
        pass  # Group already exists, move on
        
    # 2. Ensure the Log Stream exists
    try:
        client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        print(f"Created Log Stream: {log_stream_name}")
    except client.exceptions.ResourceAlreadyExistsException:
        pass  # Stream already exists, move on

    # 3. Format the log event
    # AWS CloudWatch requires timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)
    
    log_event = {
        'logGroupName': log_group_name,
        'logStreamName': log_stream_name,
        'logEvents': [
            {
                'timestamp': timestamp_ms,
                'message': log_message
            }
        ]
    }
    
    # 4. Put the log event
    try:
        response = client.put_log_events(**log_event)
        print(f"Log sent successfully! Status: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response
    except Exception as e:
        print(f"Error putting log event: {e}")
        return None

# Example Usage
if __name__ == "__main__":
    GROUP = "/apps/production-server"
    STREAM = "application-instance-01"
    MESSAGE = "[INFO] - 2026-06-22 - Application started successfully. Database connected."
    
    put_cloudwatch_log(GROUP, STREAM, MESSAGE)