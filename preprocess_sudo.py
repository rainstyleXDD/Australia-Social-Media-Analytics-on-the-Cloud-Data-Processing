# import libraries
import json
import uuid
import couchdb

# constant
UNNEED_CHAR_POS = -4
URL = 'http://admin:password@172.26.133.215:5984/'

# input and output file path
FILE_PATH = './sudo_data/rai_business_indicators_lga_2011-5781914874165984729.json'
OUT_FILE_PATH = './sudo_data/business_indicator.json'
OUT_FILE_PATH_2 = './sudo_data/lga.json'

# extract the useful data from sudo
with open(FILE_PATH, 'r') as file, open(OUT_FILE_PATH, 'w') as out_file:
    data = json.load(file)
    out_file.write('[')

    for index in range(len(data['features'])):

        # save the properties and id for each LGA place
        properties = data['features'][index]['properties']
        properties['_id'] = str(uuid.uuid4())
        json_object = json.dumps(properties)
        out_file.write(json_object)
        if index < (len(data['features']) - 1):
            out_file.write(',\n')
    out_file.write(']')

# connect to couchdb server
couch = couchdb.Server(URL)
db = couch['sudo']

# save the data in couchDB
with open(OUT_FILE_PATH, 'r') as f:
    sudo_data = json.load(f)
    db.update(sudo_data)

# Opening sudo JSON file
with open(OUT_FILE_PATH, 'r') as lga, open(OUT_FILE_PATH_2, 'w') as output:

    # returns JSON object as a dictionary
    lga_data = json.load(lga)
    lga_dict = dict()

    # extract the information of LGA
    for i in range(len(lga_data)):
        lganame = lga_data[i]['lganame'][:UNNEED_CHAR_POS].lower()
        lga_dict[lganame] = {'fullname': lga_data[i]['lganame'],
                            'state': lga_data[i]['state']}

    # write into the output file
    json_object_2 = json.dumps(lga_dict, indent=2)
    output.write(json_object_2)