import json
import csv
# Opening JSON file
input_file_name = 'Virginia_data/svi_county_GISJOIN.Virginia.raw_data.json'
f = open(input_file_name) # reading input file 
# returns JSON object as
# a dictionary
data = json.load(f)
output_file_name = 'VG_svi_data.csv'
with open(output_file_name, 'w', newline='') as csvfile:
    fieldnames = ['STATE', 'COUNTY','RPL_THEMES','Socio_Eco_RPL1', 'HouseCompDis_RPL2','MinorStat_RPL3','HouseTraonsport_RPL3']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    
    for i in data:
        writer.writerow({'STATE': i['STATE'], 'COUNTY': i['COUNTY'],'RPL_THEMES':i['RPL_THEMES'],'Socio_Eco_RPL1':i['RPL_THEME1'], 'HouseCompDis_RPL2':i['RPL_THEME2'],'MinorStat_RPL3':i['RPL_THEME3'],'HouseTraonsport_RPL3':i['RPL_THEME4']})
        print(i['STATE'],i['COUNTY'],i['M_AGE65'])
        print("=")
