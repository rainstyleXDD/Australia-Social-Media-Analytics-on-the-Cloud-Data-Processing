# import libraries
import couchdb
import json

# constant
FILE_NUM = 4
URL = 'http://admin:password@172.26.130.251:5984/'

# upload each file
for num in range(FILE_NUM):

    path = f'./curated_filter_data/tweetFiltered{num}.json'
    with open(path, 'r') as file:
        data = json.load(file)

        # save the total number of tweet in couchDB
        couch = couchdb.Server(URL)
        db = couch['twitter_data']

        # upload the data and catch the server error
        try:
            db.update(data)
        except couchdb.http.ServerError as e:
            print(f'Raise server error: {e}')
            continue
