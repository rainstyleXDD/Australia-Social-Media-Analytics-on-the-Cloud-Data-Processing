"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

# import libraries
import json
import uuid
import couchdb
import csv
from constant import const

# constant
UNNEED_CHAR_POS = -4
DB_NAMES = ['sudo_climate', 'sudo_health', 'sudo_unemp']

# input and output file path
FILE_PATH = ['./sudo_data/abs_2021census_g62_aust_lga-36315067387021157.json',
            './sudo_data/lga11_healthriskfactors_modelledestimate-7414866553980188670.json',
            './sudo_data/phidu_labour_force_lga_2016_19-1226199109284126335.json']
OUT_FILE_PATH = ['./sudo_data/go_work.json',
                './sudo_data/health_risk.json',
                './sudo_data/unemployment.json']

with open('./sudo_data/lga.json', 'r') as lga:

    # returns JSON object as a dictionary
    lga_data = json.load(lga)

lga_code = {}
with open("CG_LGA_2021_LGA_2022.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        lga_code[row['LGA_NAME_2021'].lower()] = row['LGA_CODE_2021']

lga_name ={}
for i in lga_data:
    if i in lga_code:
        lga_name[lga_code[i]] = lga_data[i]['fullname']

# extract the useful data from sudo
for i in range(len(FILE_PATH)):
    with open(FILE_PATH[i], 'r') as file, open(OUT_FILE_PATH[i], 'w') as out_file:
        data = json.load(file)
        out_file.write('[')

        for index in range(len(data['features'])):
            found_name = True

            # save the properties and id for each LGA place
            properties = data['features'][index]['properties']
            properties['_id'] = uuid.uuid4().hex
            if DB_NAMES[i] == 'sudo_climate':
                found_name = False
                if properties['lga_code_2021'] in lga_name:
                    properties['full_name'] = lga_name[properties['lga_code_2021']]
                    found_name = True
                        
            if found_name == True:          
                json_object = json.dumps(properties)
                out_file.write(json_object)
                if index < (len(data['features']) - 1):
                    out_file.write(',\n')
        out_file.write(']')

# connect to couchdb server
couch = couchdb.Server(const.URL)

# save the data in couchDB
for i in range(len(DB_NAMES)):
    with open(OUT_FILE_PATH[i], 'r') as f:
        sudo_data = json.load(f)
        # if not exist, create one
        if DB_NAMES not in couch:
            db = couch.create(DB_NAMES[i])
        else:
            db = couch[DB_NAMES[i]]
        db.update(sudo_data)

