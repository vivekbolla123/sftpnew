import traceback
import paramiko
import os
import time
import json
from sqlalchemy import create_engine
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Connect to the SQLite database
DB_USERNAME = "rmscriptuser"
DB_PASSWORD = "8BkNw9JEOHoxD6h"
DB_URL = "rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
RM_ALLOC_DB_URL = "mysql+mysqldb://"+DB_USERNAME+":"+DB_PASSWORD+"@"+DB_URL
RM_ALLOC_DB = RM_ALLOC_DB_URL+"/QP_DW_RMALLOC"
conn = create_engine(RM_ALLOC_DB)

# Specify the runid you want to select


def pad_rbd_value(rbd_value):
    return f"{rbd_value:04}"

def format_date(date_string):
    # Assuming date_string is in the format DD-MM-YYYY
    date_obj = datetime.strptime(date_string, "%d-%m-%Y")
    return date_obj.strftime("%Y%m%d")

# Define a function to generate the file content
def generate_file_content(runid, departurdate, flightno1, sector1, flightno2, sector2, rbds):
    file_content = ""
    for rbd_value in rbds:
        if int(rbds[rbd_value]) > 0:
            rbd, rbd_value = rbd_value, pad_rbd_value(int(rbds[rbd_value]))
            final_date = format_date(departurdate)
            line = f"{final_date} QP   {flightno1} {sector1} QP   {flightno2} {sector2}{' ' * (24 - len(sector2))}"
            line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
            file_content += line
    return file_content

# Function to upload the file to SFTP
def upload_to_sftp(sftp_host, sftp_port, sftp_username, sftp_password, remote_dir, local_file):
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    remote_file_path = os.path.join(remote_dir, os.path.basename(local_file))

    # Check if the remote file already exists
    
        # Upload the local file to the SFTP server
    sftp.put(local_file, remote_file_path)
    logger.info(f"{local_file} successfully uploaded")
    sftp.close()
    transport.close()

# AWS Lambda Handler Function
def lambda_handler(event, context):
    records = event['Records']
    for record in records:
        content = record['body']
        data = json.loads(content)
        runid=data["run_id"]
# Fetch data from the database for the specified runid
        data = conn.execute(f"SELECT result FROM navitaire_allocation_audit WHERE run_id = '{runid}'")
    # Generate and write data to the AUUPDATE file
        file_name = "/tmp/AUUPDATE"  # Use Lambda's /tmp directory
        with open(file_name, 'w') as file:
            for row in data:
                runid, departurdate, flightno1, sector1, flightno2, sector2 = runid, "07-09-2023", "1102", "BOMHYD", "2365", "HYDDEL"
                rbds = json.loads(row[0])
                file_content = generate_file_content(runid, departurdate, flightno1, sector1, flightno2, sector2, rbds)
                file.write(file_content)

        # Periodically check if the remote file exists (every 15 seconds)
        divide_file(file_name)
    return {
        'statusCode': 200,
        'body': 'success'
    }

    # This code will run indefinitely, checking for the file every 15 seconds.
def upload_to_sftp(local_file):
    sftp_host = "sftp.akasaair.com"
    sftp_port = 10022
    sftp_username = "0K_MFT"
    sftp_password = "Ksm@23wC"
    remote_dir = "/TEST/ESC"
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    remote_file_path = os.path.join(remote_dir, "AUUPDATE")
    
    timeout = 0
    refresh_time=10
    while timeout < 780:
            # Poll the SQS queue for messages
            try:
                # Check if the target file already exists on the SFTP server
                try:
                    sftp.stat(os.path.join(
                        remote_dir, "AUUPDATE"))
                    print("File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                    logger.info(
                        f"File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                    # Wait for 30 seconds before checking again
                    time.sleep(refresh_time)
                    timeout = timeout + refresh_time
                    continue
                except FileNotFoundError:
                    pass
                sftp.put(local_file, remote_file_path)
                sftp.chdir(remote_dir)
                sftp.open("AUUPDATE.TAG", 'w').close()
                sftp.close()
                transport.close()
                print(f"{local_file} successfully uploaded")
                logger.info(f"{local_file} successfully uploaded")
                break
            except Exception as e:
                # Handle any exceptions or errors
                traceback.print_exc()
                logger.info(f"Error processing message: {str(e)}")
                break
    if timeout == 780:
        logger.info("Time limit exceeded 13 minutes")
    # Check if the remote file already exists
    
        # Upload the local file to the SFTP server
    
    
def divide_file(file_path, chunk_size_mb=40):
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    file_size = os.path.getsize(file_path)

    if file_size <= chunk_size:
        print(f"The file '{file_path}' is already smaller than {chunk_size_mb}MB.")
        return

    with open(file_path, 'rb') as input_file:
        base_filename = os.path.splitext(file_path)[0]
        chunk_number = 1
        while True:
            chunk_data = input_file.read(chunk_size)
            if not chunk_data:
                break
            
            # Check if the chunk ends with a newline character
            if chunk_data.endswith(b'\n'):
                # If it does, write the chunk to the current file
                with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                    output_file.write(chunk_data)
            else:
                # If it doesn't, find the last newline character position
                last_newline_pos = chunk_data.rfind(b'\n')
                if last_newline_pos != -1:
                    # Write the chunk up to the last newline character
                    with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                        output_file.write(chunk_data[:last_newline_pos + 1])
                    
                    # Seek back to the position after the last newline character
                    input_file.seek(-(len(chunk_data) - last_newline_pos - 1), os.SEEK_CUR)
                else:
                    # If there are no newline characters, write the whole chunk
                    with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                        output_file.write(chunk_data)
            upload_to_sftp(f"{base_filename}_part{chunk_number}")
            chunk_number += 1

        print(f"File '{file_path}' has been divided into {chunk_number - 1} chunks.")

lambda_handler("event", "context")