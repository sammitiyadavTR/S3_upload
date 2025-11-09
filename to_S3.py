import os  
import boto3  
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

try:  
    S3_CLIENT = boto3.client('s3')  
except (NoCredentialsError, PartialCredentialsError):  
    print("AWS credentials not found. Please configure them.")  
    S3_CLIENT = None
    
def delete_file(bucket_name, s3_object_key):  
    """  
    Deletes a file (object) from an S3 bucket.
  
    Args:  
        bucket_name (str): The name of the S3 bucket.  
        s3_object_key (str): The key of the object to delete.
      
    Returns:  
        bool: True if deletion was successful, False otherwise.  
    """  
    if not S3_CLIENT:  
        print("S3 client is not initialized. Cannot delete.")  
        return False
    try:  
        S3_CLIENT.delete_object(Bucket=bucket_name, Key=s3_object_key)  
        print(f"File '{s3_object_key}' deleted from '{bucket_name}'.")  
        return True  
    except ClientError as e:  
        print(f"An error occurred during deletion: {e}")  
        return False

def read_file(bucket_name, s3_object_key):

    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=s3_object_key)
    file_content = response['Body'].read().decode('utf-8')
    print(f"Content of '{s3_object_key}':\n{file_content}")
import boto3  
from botocore.exceptions import ClientError  
from datetime import datetime, timedelta, timezone  # <-- Make sure these are imported
  
  
def list_recent_s3_files(bucket_name, folder_prefix, days=7):  
    """  
    Lists file names in an S3 folder modified within a recent time window.
  
    Args:  
        bucket_name (str): The name of the S3 bucket.  
        folder_prefix (str): The prefix representing the "folder" path in S3.  
        days (int): The number of days to look back for recent files.  
                      Defaults to 7 (one week).
  
    Returns:  
        list: A list of object keys (file names) modified within the time  
              window. Returns None on error.  
    """  
    if not S3_CLIENT:  
        print("S3 client is not initialized. Cannot list files.")  
        return None
  
    recent_files = []  
    old_files = []
    try:  
        # Define the time window for filtering.  
        # It's crucial to use timezone-aware datetimes for correct comparison,  
        # as S3 returns LastModified in UTC.  
        time_threshold = datetime.now(timezone.utc) - timedelta(days=days)
        print(time_threshold)
        month_threshold = datetime.now(timezone.utc) - timedelta(days=30)
  
        paginator = S3_CLIENT.get_paginator('list_objects_v2')  
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
  
        for page in pages:  
            if 'Contents' in page:  
                for obj in page['Contents']:  
                    # Skip objects that represent folders  
                    if obj['Key'].endswith('/'):  
                        continue
  
                    # The 'LastModified' value from boto3 is already a timezone-aware datetime object  
                    last_modified_time = obj['LastModified']
                    print(last_modified_time)
  
                    # Check if the object was modified after our time threshold  
                    if last_modified_time >= time_threshold:  
                        recent_files.append(obj['Key'])
                    elif last_modified_time <= month_threshold: 
                        old_files.append(obj['Key'])
                    else: 
                        print("No recent files found to be moved")    
  
        for file_obj in old_files: 
            #s3_object_key = f"{folder_prefix}{file}"
            #delete_file(bucket_name, file_obj)
        
        return recent_files
  
    except ClientError as e:  
        print(f"An error occurred while listing files: {e}")  
        return None

def move(bucket_name, temp_file_name_obj, file_name_obj):
    
   if not S3_CLIENT:  
    print("S3 client is not initialized. Cannot list files.")  
    return None 
   
   try: 
    S3_CLIENT.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key':  temp_file_name_obj},
    Key=file_name_obj
    )
    return True
   except ClientError as e:  
    print(f"An error occurred moving files: {e}")  
    return None 

def main():  
    """  
    Main function to drive the S3 file management script.  
     """ 
  
    # --- Configuration ---  
    bucket_name = "a208376-raiassistant-openarena"  
    s3_folder_prefix = "RAIHAssitant/"
    s3_temp_prefix = "temp/"
  
    # --- Check new files uploaded ---  
    # 1. List files uploaded in past week
    recent_files = list_recent_s3_files(bucket_name, s3_temp_prefix)
    if recent_files is not None:  
       count = len(recent_files)  
       print(f"Found {count} file(s) in '{s3_temp_prefix}' in bucket '{bucket_name}':")  
       for file_name in recent_files:  
           print(f"- {file_name}")  
    else:  
     # Error message is already printed inside the function  
        print(f"Could not retrieve file list from '{bucket_name}/{s3_temp_prefix}'.")
    # 2. review new files  
    file_name_in = input("Input the file name after('/') to be read from the list of files: ")
    temp_file_name_obj = f"{s3_temp_prefix}{file_name_in}"
    read_file(bucket_name, temp_file_name_obj)
    
    reviewed = input("Are you okay to move this file?y/n: ")
    if reviewed == 'y': 
       # 4. move the file from temp to main
      file_name_obj = f"{s3_folder_prefix}{file_name_in}"
      move(bucket_name, temp_file_name_obj, file_name_obj)
      
    # 5. delete the 30 days old files in temp
    
if __name__ == "__main__":  
    main()      