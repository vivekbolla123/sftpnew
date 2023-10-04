from datetime import datetime
import json
from sqlalchemy import create_engine

DB_USERNAME = "rmscriptuser"
DB_PASSWORD = "8BkNw9JEOHoxD6h"
DB_URL = "rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
RM_ALLOC_DB_URL = "mysql+mysqldb://"+DB_USERNAME+":"+DB_PASSWORD+"@"+DB_URL
RM_ALLOC_DB = RM_ALLOC_DB_URL+"/QP_DW_RMALLOC"
conn = create_engine(RM_ALLOC_DB)

# Specify the runid you want to select
target_runid = '13df412e-5762-485b-a259-469cc9fab5f0'

# Fetch data from the database for the specified runid
data = conn.execute(f"SELECT result FROM navitaire_allocation_audit WHERE run_id = '{target_runid}'")



def pad_rbd_value(rbd_value):
    return f"{rbd_value:04}"

def format_date(date_string):
    # Assuming date_string is in the format DD-MM-YYYY
    date_obj = datetime.strptime(date_string, "%d-%m-%Y")
    return date_obj.strftime("%Y%m%d")
# Define a function to generate the file content
def generate_file_content( departurdate, flightno, sector, rbds):
    file_content = ""
    for rbd_value in rbds:
        if int(rbds[rbd_value])>0:
            rbd, rbd_value = rbd_value,pad_rbd_value(int(rbds[rbd_value]))
            final_date=format_date(departurdate)
            line = f"{final_date} QP   {flightno} {sector}{' ' * (40 - len(sector))}"
            line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
            file_content += line
    return file_content

# Write data to a file without extension
file_name = "AUUPDATE"
with open(file_name, 'w') as file:
    
    departurdate, flightno, sector ="05-10-2023","1108","AMDBOM"
    rbds={"S": -2, "G": -2, "Y": 189, "B": 162, "C": 159, "D": 156, "E": 153, "F": 150, "H": 147, "I": 147, "J": 144, "K": 141, "M": 138, "N": 138, "O": 135, "Q": 135, "Q0": 132, "Q1": 132, "Q2": 132, "Q3": 132, "Q4": 132, "Q5": 132, "Q6": 132, "Q7": 132, "Q8": 132, "Q9": 132, "R0": 132, "R1": 129, "R2": 126, "R3": 126, "R4": 123, "R5": 123, "R6": 120, "R7": 120, "R8": 117, "R9": 117, "T0": 114, "T1": 114, "T2": 111, "T3": 111, "T4": 108, "T5": 108, "T6": 105, "T7": 105, "T8": 102, "T9": 102, "U0": 99, "U1": 99, "U2": 96, "U3": 96, "U4": 0, "U5": 0, "U6": 0, "U7": 0, "U8": 0, "U9": 0, "V0": 0, "V1": 0, "V2": 0, "V3": 0, "V4": 0, "V5": 0, "V6": 0, "V7": 0, "V8": 0, "V9": 0, "Z0": 93, "Z1": 93, "Z2": 92, "Z3": 92, "Z4": 0, "Z5": 0, "Z6": 0, "Z7": 0, "Z8": 0, "Z9": 0, "ZA": 0, "ZB": 0, "ZC": 0, "ZD": 0, "ZE": 0, "ZF": 0, "ZG": 0, "ZH": 0, "ZI": 0, "ZJ": 0, "W": -2, "W0": -2, "W1": -2, "W2": -2, "W3": -2, "W4": -2, "W5": -2, "W6": -2, "W7": -2, "W8": -2, "W9": -2, "P": -1, "L": -1}
    
    file_content = generate_file_content(departurdate, flightno, sector, rbds)
    file.write(file_content)

# Close the database connection
# conn.close()
