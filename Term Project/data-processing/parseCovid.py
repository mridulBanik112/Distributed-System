
import json


files = ['Virginia_data/covid_county.Virginia.raw_data.json']
i = 1
years = ["1/22/20", "1/31/21", "1/31/22"]
with open('VGparsed.csv', 'w+') as f:
    for file in files:
        with open(file, "r") as f2:
            jdata  = json.loads(f2.read())
            #f.write('state, county, totalDeath, totalCases, year\n')
            for entry in jdata:
                if entry['dateString'] ==  "1/22/20":
                    f.write(entry['state'] + "," + entry['county'] + "," + str(entry['totalDeathCount']) + "," +  str(entry['totalCaseCount']) + "," + entry['dateString'] + '\n')
                elif entry['dateString'] ==  "1/31/21":
                    f.write(entry['state'] + "," + entry['county'] + "," + str(entry['totalDeathCount']) + "," +  str(entry['totalCaseCount']) + "," + entry['dateString'] + '\n')
                elif entry['dateString'] ==  "1/31/22":
                    f.write(entry['state'] + "," + entry['county'] + "," + str(entry['totalDeathCount']) + "," +  str(entry['totalCaseCount']) + "," + entry['dateString'] + '\n')

