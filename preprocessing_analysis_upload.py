# import libraries
import json
from mpi4py import MPI
from datetime import datetime
import pytz
import uuid
import couchdb
from collections import Counter, defaultdict
from copy import deepcopy

# constant
URL = 'http://admin:password@172.26.133.215:5984/'

SYD_TIME = {'NSW': 'Australia/Sydney',
            'VIC': 'Australia/Sydney',
            'QLD': 'Australia/Sydney',
            'ACT': 'Australia/Sydney',
            'TAS': 'Australia/Sydney'}

ADE_TIME  ={'SA': 'Australia/Adelaide',
            'NT': 'Australia/Adelaide'}

PER_TIME = {'WA': 'Australia/Perth'}

CLIMATE_KEYWORD = ["climate", "ClimateCrisis", "ClimateCriminals",
                   "ClimateActionNow", "ClimateApe", "climatechange",
                   "ClimateStrike", "Climate", "ClimateEmergency", "ClimateChange"]

INI_TWEET = {
    '_id': 0,
    'location': {'state': 0,
                'lga': 0},
    'sentiment': 0,
    'date':{'year': 0,
            'month': 0,
            'day': 0,
            'hour': 0,
            'min': 0,
            'sec': 0},
    'mention_coffee': False,
    'mention_work': False,
    'mention_enter': False,
    'mention_climate': False,
    'mention_night': False
}

# Filter the tweet which not relavant to the scenario
def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    # find the number of file for each rank
    file_num = 9
    if rank == 0:
        file_num = 8

    lga_tweet = Counter()
    lga_tweet_latenight = Counter()
    lga_tweet_climate = Counter()    
   

    file_path_w = './curated_filter_data2/tweetFiltered'+ str(rank) + '.json'
    with open(file_path_w, 'w') as outfile:
        in_scenario = 0
        count = 0
        outfile.write('[')
        # process all files
        for i in range(file_num):
            file_path_r = './curated_data/tweetsProcessed' + str(rank) + '_' + str(i) + '.json'
            print(rank, "reading", i)

            with open(file_path_r, 'r') as file:

                # returns JSON object as a dictionary
                data = json.load(file)
                
                for tweet in data:

                    # initialise the dictionary
                    tweet_dict = deepcopy( INI_TWEET)
                    # change the timezone based on state
                    state = tweet['state'].upper()

                    # create a datetime object representing the UTC time
                    utc_time = datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    
                    if state in SYD_TIME:
                        # set the timezone for Sydney
                        city_tz = pytz.timezone(SYD_TIME[state])

                    elif state in ADE_TIME:
                        # set the timezone for Adelaide
                        city_tz = pytz.timezone(ADE_TIME[state])

                    elif state in PER_TIME:
                        # set the timezone for Perth
                        city_tz = pytz.timezone(PER_TIME[state])

                    # convert the UTC time to local time
                    local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(city_tz)
                    
                    # check whether the tweet match the first scenario
                    if local_time.hour >= 23 or local_time.hour <= 5:
                        tweet_dict['mention_night'] = True
                        in_scenario += 1
                        
                    
                    # check whether the tweet match the second scenario
                    for keyword in CLIMATE_KEYWORD:
                        if (keyword in tweet['value']['tokens']) or (keyword in tweet['value']['tags']):
                            tweet_dict['mention_climate'] = True
                            in_scenario += 1
                            break

                    # check this tweet whether match any scenario
                    if in_scenario == 0:
                        count += 1
                        continue

                    # if it match one of the scenario, write in the output file
                    else:
                        tweet_dict['_id'] = str(uuid.uuid4())
                        tweet_dict['location']['state'] = tweet['state']
                        tweet_dict['location']['lga'] = tweet['full_name']
                        tweet_dict['sentiment'] = tweet['sentiment']

                        # save the date and time
                        tweet_dict['date']['year'] = local_time.year
                        tweet_dict['date']['month'] = local_time.month
                        tweet_dict['date']['day'] = local_time.day
                        tweet_dict['date']['hour'] = local_time.hour
                        tweet_dict['date']['min'] = local_time.minute
                        tweet_dict['date']['sec'] = local_time.second

                        # formate the JSON file
                        if useful_tw != 0:
                            outfile.write(',\n')

                        useful_tw += 1
                        count += 1
                        json_object = json.dumps(tweet_dict)
                        outfile.write(json_object)

        # close the formated JSON file
        outfile.write(']')
                    

        # aggregate and print out the formated results
        if rank == 0:
            
            count += comm.recv(source=i, tag=1)
            
            # save the total number of tweet in couchDB
            couch = couchdb.Server(URL)
            db = couch['twitter_data']
            uid = str(uuid.uuid4())
            doc = {'_id': uid, 'total_tweet': count}
            db.save(doc)
            print('id:', uid)
            print('count:', count)

            return

            

        else:
            comm.send(count, 0, tag=1)


    return 



if __name__ == "__main__":
    main()
