# import libraries
import json
import uuid
import couchdb

# input and output file path
file_path = './rai_business_indicators_lga_2011-5781914874165984729.json'
out_file_path = './business_indicator.json'

# extract the useful data from sudo
with open(file_path, 'r') as file, open(out_file_path, 'w') as out_file:
    data = json.load(file)
    out_file.write('[')
    for index in range(len(data['features'])):
        properties = data['features'][index]['properties']
        properties['_id'] = str(uuid.uuid4())
        json_object = json.dumps(properties)
        out_file.write(json_object)
        if index < (len(data['features']) - 1):
            out_file.write(',\n')
    out_file.write(']')

# save the data in couchDB
couch = couchdb.Server('http://admin:password@172.26.131.66:5984/')
db = couch['sudo']
with open('./business_indicator.json', 'r') as f:
    sudo_data = json.load(f)
    db.update(sudo_data)