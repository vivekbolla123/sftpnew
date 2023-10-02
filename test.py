# import sqlite3

# # Connect to the SQLite database
# conn = sqlite3.connect('your_database.db')
# cursor = conn.cursor()

# # Specify the runid you want to select
# target_runid = 'your_target_runid'

# # Fetch data from the database for the specified runid
# cursor.execute("SELECT runid, departurdate, flightno, sector, rbds FROM run-flight-date-audit WHERE runid = ?", (target_runid,))
# data = cursor.fetchall()
from datetime import datetime


def pad_rbd_value(rbd_value):
    return f"{rbd_value:04}"

def format_date(date_string):
    # Assuming date_string is in the format DD-MM-YYYY
    date_obj = datetime.strptime(date_string, "%d-%m-%Y")
    return date_obj.strftime("%Y%m%d")
# Define a function to generate the file content
def generate_file_content(runid, departurdate, flightno, sector, rbds):
    file_content = ""
    for rbd_value in rbds:
        rbd, rbd_value = rbd_value,pad_rbd_value(int(rbds[rbd_value]))
        final_date=format_date(departurdate)
        line = f"{final_date} QP   {flightno} {sector}{' ' * (40 - len(sector))}"
        line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
        file_content += line
    return file_content

# Write data to a file without extension
file_name = "AUUPDATE"
with open(file_name, 'w') as file:
    
    runid, departurdate, flightno, sector, rbds = "fefe","07-09-2023","1102","BOMHYD",{"S": -2, "L": -1, "P": -1, "G": -2, "Y": 174, "B": 114, "C": 112, "D": 110, "E": 108, "F": 106, "H": 104, "I": 102, "J": 100, "K": 98, "M": 96, "N": 94, "O": 92, "Q": 90, "R0": 88, "R1": 86, "R2": 84, "R3": 82, "R4": 80, "R5": 78, "R6": 76, "R7": 74, "R8": 72, "R9": 70, "T0": 68, "T1": 66, "T2": 64, "T3": 61, "T4": 58, "T5": 54, "T6": 54, "T7": 49, "T8": 49, "T9": 42, "U0": 42, "U1": 33, "U2": 22, "U3": 22, "U4": 0, "U5": 0, "U6": 0, "U7": 0, "U8": 0, "U9": 0, "V0": 0, "V1": 0, "V2": 0, "V3": 0, "V4": 0, "V5": 0, "V6": 0, "V7": 0, "V8": 0, "V9": 0, "Z0": 0, "Z1": 0, "Z2": 0, "Z3": 0, "Z4": 0, "Z5": 0, "Z6": 0, "Z7": 0, "Z8": 0, "Z9": 0, "W": -2}
    file_content = generate_file_content(runid, departurdate, flightno, sector, rbds)
    file.write(file_content + '\n')

# Close the database connection
# conn.close()
