file_name = "AUUPDATE"


with open(file_name, 'w') as file:
    for row in data:
        runid, departurdate, flightno1, sector1,flightno2,sector2 = "fefe","07-09-2023","1102","BOMHYD","2365","HYDDEL"
        rbds=json.loads(row[0])
        file_content = generate_file_content(runid, departurdate, flightno1,sector1,flightno2, sector2,rbds)
        if file_content!=-1:
            file.write(file_content)
    