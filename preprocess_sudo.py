# import libraries
import json
import uuid
import couchdb

# input and output file path
file_path = './rai_business_indicators_lga_2011-5781914874165984729.json'
out_file_path = './business_indicator.json'
out_file_path_2 = './lga.json'

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
couch = couchdb.Server('http://admin:password@172.26.133.215:5984/')
db = couch['sudo']
with open('./business_indicator.json', 'r') as f:
    sudo_data = json.load(f)
    db.update(sudo_data)

# Opening sudo JSON file
with open(out_file_path, 'r') as lga, open(out_file_path_2, 'w') as output:

    # returns JSON object as a dictionary
    lga_data = json.load(lga)
    lga_dict = dict()

    for i in range(len(lga_data)):
        lganame = lga_data[i]['lganame'][:-4].lower()
        lga_dict[lganame] = {'fullname': lga_data[i]['lganame'],
                            'state': lga_data[i]['state']}

    json_object_2 = json.dumps(lga_dict, indent=2)
    output.write(json_object_2)