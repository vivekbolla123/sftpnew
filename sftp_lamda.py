import json
import os
import sys
import boto3
import warnings
import paramiko
import time
import logging
import configsettings
from datetime import datetime
from sqlalchemy import create_engine

warnings.filterwarnings('ignore')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

try:
    conn = create_engine(DB_WR_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    data = {
        "starttime": datetime.now(),
        "refresh_time": 30,
        "chunk_size": 4 * 1024 * 1024,  # 4MB in bytes
        "timeout": 900,
        "userName": "SYS2",

    }
    records = event['Records']
    processor = DataProcessor(data)
    for record in records:
        content = record['body']
        content = json.loads(content)
        receiptHandle = record['receiptHandle']
        processor.sendsftp(content, receiptHandle, conn)

    return {
        'statusCode': 200,
        'body': 'success'
    }


class DataProcessor:
    def __init__(self, configData):
        # Define your instance variables here
        self.starttime = configData["starttime"]
        self.refresh_time = configData["refresh_time"]
        self.chunk_size = configData["chunk_size"]
        self.sftp_host = configsettings.SFTP_HOST
        self.sftp_port = configsettings.SFTP_PORT
        self.sftp_username = configsettings.SFTP_USERNAME
        self.sftp_password = configsettings.SFTP_PASSWORD
        self.sftp_destination_dir = configsettings.SFT_DES_DIR
        self.sftp_target_file = configsettings.SFTP_TARG_FILE
        self.AWS_BUCKET_NAME = configsettings.AWS_BUCKET_NAME
        self.QUEUE_NAME = configsettings.QUEUE_NAME
        self.AWS_SQS_QUEUE_URL = configsettings.AWS_SQS_QUEUE_URL
        self.CHUNKS_PATH=configsettings.AWS_BUCKET_CHUNKS_PATH
        self.timeout = configData["timeout"]
        self.userName = configData["userName"]

    def sendsftp(self, messageBody, receiptHandle, conn):
        sqs = boto3.client('sqs')
        s3 = boto3.client('s3')
        timeout = 0
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(username=self.sftp_username,
                          password=self.sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        while timeout < self.timeout:
            # Poll the SQS queue for messages
            try:
                # Check if the target file already exists on the SFTP server
                try:
                    sftp.stat(os.path.join(
                        self.sftp_destination_dir, self.sftp_target_file))
                    logger.info(
                        f"File {self.sftp_target_file} already exists on the SFTP server. Waiting for deletion...")
                    # Wait for 30 seconds before checking again
                    time.sleep(self.refresh_time)
                    timeout = timeout + self.refresh_time
                    continue
                except FileNotFoundError:
                    pass

                # Retrieve file path and bucket name from message attributes
                file_path = messageBody

                # Download the file from S3
                file_name = os.path.basename(file_path)
                local_file_path = '/tmp/' + file_name
                s3.download_file(
                    self.AWS_BUCKET_NAME, f"{self.CHUNKS_PATH}/{file_name}", local_file_path)
                # Connect to the SFTP server and upload the file
                sftp.put(local_file_path, os.path.join(
                    self.sftp_destination_dir, self.sftp_target_file))
                logger.info(f"{file_name} successfully uploaded")
                sftp.close()
                transport.close()
                # If successful, delete the file from S3
                s3.delete_object(Bucket=self.AWS_BUCKET_NAME,
                                 Key=f"{self.CHUNKS_PATH}/{file_name}")

                # Delete the message from the SQS queue
                sqs.delete_message(
                    QueueUrl=self.AWS_SQS_QUEUE_URL, ReceiptHandle=receiptHandle)
                endtime = datetime.now()
                query = f'''INSERT INTO auchunks_sftp_audit (chunk_name,start_time, end_time,user) VALUE ("{messageBody}","{self.starttime}","{endtime}","{self.userName}")'''
                conn.execute(query)
                break
            except Exception as e:
                # Handle any exceptions or errors
                logger.info(f"Error processing message: {str(e)}")
                break

        if timeout == self.timeout:
            logger.info("Time limit exceeded 15 minutes")
            
    def create_chunks(self):
        data=self.createfile()
        data = audatframe.to_csv(index=False, encoding='utf-8')
            
        if data is None:
            print("Unable to read the file with any of the specified encodings.")
            return

        # Split the data into header and rows
        header, data = data.split('\n', 1)
        header += '\n'  # Add a newline character to the header

        chunk_number = 1
        current_chunk_size = 0
        current_chunk_data = ""
        chunk_record_size=0

        for line in data.splitlines():
            line_size = len(line)
            if current_chunk_size + line_size + len(header) <= self.chunk_size:
                current_chunk_data += line + '\n'
                current_chunk_size += line_size
                chunk_record_size+=1
            else:
                self.send_chunks(chunk_number, header + current_chunk_data)
                self.totalrecords += chunk_record_size
                chunk_record_size=0
                chunk_number += 1
                current_chunk_data = line + '\n'
                current_chunk_size = line_size

        # Create the last chunk if there is any remaining data
        if current_chunk_data:
            self.send_chunks(chunk_number, header + current_chunk_data)
            self.totalrecords += chunk_record_size
        endtime=datetime.now()
        query=f'''INSERT INTO auchunks_upload_audit (run_id,start_time, end_time,user,chunks,records) VALUE ("{self.runId}","{self.starttime}","{endtime}","SYS2","{self.nochunks}","{self.totalrecords}")'''
        self.wrconn.execute(query)
        
    def send_chunks(self, chunk_number, data):
        s3_object_key = f'prod/rm-au-chunks/chunk_{chunk_number}_{self.runId}.csv'
        self.s3.put_object(Bucket=self.AWS_BUCKET_NAME, Key=s3_object_key, Body=data)
        # Send data to SQS
        message_body = f"chunk_{chunk_number}_{self.runId}.csv"
        self.sqs.send_message(QueueUrl=self.AWS_SQS_QUEUE_URL, MessageBody=message_body)
        self.nochunks=self.nochunks+1
        
    
    def pad_rbd_value(self,rbd_value):
        return f"{rbd_value:04}"

    def format_date(self,date_string):
        # Assuming date_string is in the format DD-MM-YYYY
        date_obj = datetime.strptime(date_string, "%d-%m-%Y")
        return date_obj.strftime("%Y%m%d")
    # Define a function to generate the file content
    def generate_file_content(self,runid, departurdate, flightno1,sector1,flightno2, sector2, rbds):
        file_content = ""
        for rbd_value in rbds:
            rbd, rbd_value = rbd_value,self.pad_rbd_value(int(rbds[rbd_value]))
            final_date=self.format_date(departurdate)
            line = f"{final_date} QP   {flightno1} {sector1} QP   {flightno2} {sector2}{' ' * (24 - len(sector2))}"
            line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
            file_content += line
        return file_content

    # Write data to a file without extension
    def createfile(self):
        file_name = "AUUPDATE"
        temp_dir = '/tmp'  # Lambda's writable directory for temporary files
        temp_filename = os.path.join(temp_dir, file_name)
        data = conn.execute(f"SELECT result FROM navitaire_allocation_audit WHERE run_id = '98119c3c-c965-4dbf-a974-9195524e60ee'")

        with open(temp_filename, 'w') as file:
            for row in data:
                runid, departurdate, flightno1, sector1,flightno2,sector2 = "fefe","07-09-2023","1102","BOMHYD","2365","HYDDEL"
                rbds=json.loads(row[0])
                file_content = self.generate_file_content(runid, departurdate, flightno1,sector1,flightno2, sector2,rbds)
                file.write(file_content)
        return temp_filename
