"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

# import libraries
import couchdb
import json
from constant import const

# constant
FILE_NUM = 4
DB_NAME = 'all_twitter'

# upload each file
for num in range(FILE_NUM):

    path = f'./curated_filter_data/tweetFiltered{num}.json'
    with open(path, 'r') as file:
        data = json.load(file)

        # save the total number of tweet in couchDB
        couch = couchdb.Server(const.URL)

        # if not exist, create one
        if DB_NAME not in couch:
            db = couch.create(DB_NAME)
        else:
            db = couch[DB_NAME]

        # upload the data and catch the server error
        try:
            db.update(data)
        except couchdb.http.ServerError as e:
            print(f'Raise server error: {e}')
            continue
