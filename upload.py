import os  
import boto3  
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
  
# It's good practice to initialize the client once and reuse it.  
# This code assumes your AWS credentials and region are configured  
# (e.g., via environment variables, IAM role, or ~/.aws/credentials).  
try:  
    S3_CLIENT = boto3.client('s3')  
except (NoCredentialsError, PartialCredentialsError):  
    print("AWS credentials not found. Please configure them.")  
    S3_CLIENT = None
  
def upload_file(bucket_name, local_file_path, s3_object_key):  
    """  
    Uploads a file to an S3 bucket.
  
    Args:  
        bucket_name (str): The name of the target S3 bucket.  
        local_file_path (str): The path to the local file to upload.  
        s3_object_key (str): The desired key (path/filename) for the object in S3.
  
    Returns:  
        bool: True if upload was successful, False otherwise.  
    """  
    if not S3_CLIENT:  
        print("S3 client is not initialized. Cannot upload.")  
        return False
          
    # Ensure the local file exists before attempting to upload  
    if not os.path.exists(local_file_path):  
        print(f"Error: The file '{local_file_path}' was not found.")  
        return False
  
    try:  
        S3_CLIENT.upload_file(local_file_path, bucket_name, s3_object_key)  
        print(f"File '{local_file_path}' uploaded to '{bucket_name}/{s3_object_key}'.")  
        return True  
    except ClientError as e:  
        print(f"An error occurred during upload: {e}")  
        return False  
    except FileNotFoundError:  
        print(f"Error: The file '{local_file_path}' was not found.")  
        return False
  
    try:  
        S3_CLIENT.delete_object(Bucket=bucket_name, Key=s3_object_key)  
        print(f"File '{s3_object_key}' deleted from '{bucket_name}'.")  
        return True  
    except ClientError as e:  
        print(f"An error occurred during deletion: {e}")  
        return False
  
def list_s3_folder_files(bucket_name, folder_prefix):  
    """  
    Lists all file names within a specified "folder" (prefix) in an S3 bucket.
  
    Args:  
        bucket_name (str): The name of the S3 bucket.  
        folder_prefix (str): The prefix representing the "folder" path in S3.  
                             Ensure it ends with a '/' for folder-like behavior.  
    Returns:  
        list: A list of object keys (file names). Returns None on error.  
    """  
    if not S3_CLIENT:  
        print("S3 client is not initialized. Cannot list files.")  
        return None
          
    file_names = []  
    try:  
        paginator = S3_CLIENT.get_paginator('list_objects_v2')  
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
  
        for page in pages:  
            if 'Contents' in page:  
                for obj in page['Contents']:  
                    # Optional: Exclude the folder "object" itself if it exists  
                    if not obj['Key'].endswith('/'):  
                        file_names.append(obj['Key'])  
        return file_names  
    except ClientError as e:  
        print(f"An error occurred listing files: {e}")  
        return None
  
def begin_upload(files_to_be_uploaded):  
    """  
    Main function to drive the S3 file management script.  
    """  
    # Check if the S3 client was initialized successfully  
    if not S3_CLIENT:  
        print("Exiting due to credential issues.")  
        return
  
    # --- Configuration ---  
    bucket_name = "a208376-raiassistant-openarena"  
    s3_temp_prefix = "temp/"
  
    # --- User Input for File Upload ---  
    # 1. Accept a file path from user input  
    #local_file_path = input("input the file path to be uploaded: ")
  
    # 2. Use the input to set the local_file_path variable  
    # We strip any surrounding quotes or spaces for cleaner input  
    successful_uploads = 0  
    failed_uploads = 0
  
  
    print("\n--- Starting Bulk Upload ---")  
    for path in files_to_be_uploaded:  
        if not path: continue  
        path = path.strip().strip("'\"")
        file_name = os.path.basename(path)  
        upload_s3_object_key = f"{s3_temp_prefix}{file_name}"  
        if upload_file(bucket_name, path, upload_s3_object_key):  
            successful_uploads += 1  
        else:  
            failed_uploads += 1
            
    print("\n--- Upload Summary ---")  
    print(f"Successfully uploaded: {successful_uploads} file(s).")  
    print(f"Failed to upload: {failed_uploads} file(s).")      
  
    # Proceed only if the user provided a path
  
    # --- List Files in S3 Folder ---  
    print("\n--- Listing Files in S3 Bucket ---")  
    files = list_s3_folder_files(bucket_name, s3_temp_prefix)
  
    if files is not None:  
       count = len(files)  
       print(f"Found {count} file(s) in '{s3_temp_prefix}' in bucket '{bucket_name}':")  
       for file_name in files:  
         print(f"- {file_name}")  
    else:  
         # Error message is already printed inside the function  
         print(f"Could not retrieve file list from '{bucket_name}/{s3_temp_prefix}'.")
    
    return (successful_uploads, failed_uploads, files)  
if __name__ == "__main__":  
    main()  