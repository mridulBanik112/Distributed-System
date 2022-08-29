import csv

files = ['VGparsed.csv']
dict = {}
for file in files:
    with open(file, newline='') as f2:
        csvdata = csv.reader(f2, delimiter=',')
        for col in csvdata:
            state = col[0]
            state = state.upper()
            county = col[1]
            deaths = col[2]
            cases = col[3]
            date = col[4]
            pair = state + "," + county
            value = cases + "," + deaths
            if pair in dict:
                dict[pair].append(value)
            else:
                dict[pair] = [value]


#print(dict) 
string = ''
with open('finalVG.csv', 'w+') as f:
    f.write('STATE,COUNTY,cases2020,deaths2020,cases2021,deaths2021,cases2022,deaths2022\n')
    for key in dict:
      
        string = ','.join(dict[key])

        f.write("%s,%s\n"%(key,string))
