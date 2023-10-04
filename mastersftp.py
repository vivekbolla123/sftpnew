import paramiko
import os
import time
import json
from sqlalchemy import create_engine
from datetime import datetime

# Connect to the SQLite database
DB_USERNAME = "rmscriptuser"
DB_PASSWORD = "8BkNw9JEOHoxD6h"
DB_URL = "rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
RM_ALLOC_DB_URL = "mysql+mysqldb://"+DB_USERNAME+":"+DB_PASSWORD+"@"+DB_URL
RM_ALLOC_DB = RM_ALLOC_DB_URL+"/QP_DW_RMALLOC"
conn = create_engine(RM_ALLOC_DB)

# Specify the runid you want to select
target_runid = '98119c3c-c965-4dbf-a974-9195524e60ee'

# Fetch data from the database for the specified runid
data = conn.execute(f"SELECT result FROM navitaire_allocation_audit WHERE run_id = '{target_runid}'")

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

    sftp.close()
    transport.close()

# AWS Lambda Handler Function
def lambda_handler(event, context):
    # SFTP server configuration
    sftp_host = "sftp.akasaair.com"
    sftp_port = 10022
    sftp_username = "0K_MFT"
    sftp_password = "Ksm@23wC"
    remote_dir = "/TEST/ESC"  # Modify this to your SFTP directory

    # Generate and write data to the AUUPDATE file
    file_name = "/tmp/AUUPDATE"  # Use Lambda's /tmp directory
    with open(file_name, 'w') as file:
        for row in data:
            runid, departurdate, flightno1, sector1, flightno2, sector2 = "fefe", "07-09-2023", "1102", "BOMHYD", "2365", "HYDDEL"
            rbds = json.loads(row[0])
            file_content = generate_file_content(runid, departurdate, flightno1, sector1, flightno2, sector2, rbds)
            file.write(file_content)

    # Periodically check if the remote file exists (every 15 seconds)
    transport=paramiko.Transport((sftp_host, sftp_port)) 
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    remote_file_path = os.path.join(remote_dir, os.path.basename(file_name))
    while True:
        # Check the existence of the remote file
        
            try:
                    sftp.stat(remote_file_path)
                    # Wait for 30 seconds before checking again
                    time.sleep(10)
                    continue
            except FileNotFoundError:
                pass
            upload_to_sftp(sftp_host, sftp_port, sftp_username, sftp_password, remote_dir, file_name)

    # This code will run indefinitely, checking for the file every 15 seconds.

lambda_handler("event", "context")