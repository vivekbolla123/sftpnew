import os
import traceback
import paramiko
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sftp_host = "sftp.akasaair.com"
sftp_port = 10022
sftp_username = "0K_MFT"
sftp_password = "Ksm@23wC"
remote_dir = "/TEST/ESC"
def upload_to_sftp(local_file):
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

if __name__ == "__main__":
    file_path = input("Enter the path to the file: ")
    if not os.path.isfile(file_path):
        print(f"'{file_path}' does not exist.")
    else:
        divide_file(file_path)
