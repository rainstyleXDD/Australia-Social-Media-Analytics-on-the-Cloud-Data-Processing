"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

# import libraries
import json
import pytz
import uuid
import couchdb
from mpi4py import MPI
from copy import deepcopy
from constant import const
from datetime import datetime

# constant
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

START_NIGHT = 22
END_NIGHT = 5
FILE_NUM = 9

# Filter the tweet which not relavant to the scenario
def filter_data(comm):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # find the number of file for each rank
    file_num = FILE_NUM
    if rank == 0:
        file_num = FILE_NUM - 1
    
    file_path_w = './curated_filter_data/tweetFiltered'+ str(rank) + '.json'
    with open(file_path_w, 'w') as outfile:
        outfile.write('[')

        # count the number of twitter
        count = 0
        useful_tw = 0

        # process all files
        for i in range(file_num):
            file_path_r = './curated_data/tweetsProcessed' + str(rank) + '_' + str(i) + '.json'

            with open(file_path_r, 'r') as file:

                # returns JSON object as a dictionary
                data = json.load(file)
                
                for tweet in data:

                    # initialise the dictionary
                    tweet_dict = deepcopy(INI_TWEET)

                    # use to decide whether the tweet match the scenarios
                    in_scenario = 0

                    # change the timezone based on state
                    state = tweet['state'].upper()

                    # create a datetime object representing the UTC time
                    utc_time = datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    
                    if state in const.SYD_TIME:
                        # set the timezone for Sydney
                        city_tz = pytz.timezone(const.SYD_TIME[state])

                    elif state in const.ADE_TIME:
                        # set the timezone for Adelaide
                        city_tz = pytz.timezone(const.ADE_TIME[state])

                    elif state in const.PER_TIME:
                        # set the timezone for Perth
                        city_tz = pytz.timezone(const.PER_TIME[state])

                    # convert the UTC time to local time
                    local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(city_tz)
                    
                    # check whether the tweet match the night scenario
                    if local_time.hour >= START_NIGHT or local_time.hour <= END_NIGHT:
                        tweet_dict['mention_enter'] = True
                        tweet_dict['mention_night'] = True
                        in_scenario += 1
                    
                    # check whether the tweet match the coffee scenario
                    for keyword in const.COFFEE_KEYWORD:
                        if keyword in tweet['value']['tokens'].lower():
                            tweet_dict['mention_coffee'] = True
                            in_scenario += 1
                            break
                    
                    # check whether the tweet match the work scenario
                    for keyword in const.WORK_KEYWORD:
                        if keyword in tweet['value']['tokens'].lower():
                            tweet_dict['mention_work'] = True
                            in_scenario += 1
                            break
                    
                    # check whether the tweet match the climate scenario
                    for keyword in const.CLIMATE_KEYWORD:
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
                        tweet_dict['_id'] = uuid.uuid4().hex
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
    print(f'rank{rank}: {useful_tw}')
    return count

# Process of the master processor
def master_tweet_processor(comm):
    result = filter_data(comm)

    # send result to master processor
    all_result = comm.gather(result, root=0)

    # sum the total tweet count
    count_tweet = 0
    for num in all_result:
        count_tweet += num
    
    # save the total number of tweet in couchDB
    couch = couchdb.Server(const.URL)
    db_name = 'twitter_data'
    
    # if not exist, create one
    if db_name not in couch:
        db = couch.create(db_name)
    else:
        db = couch[db_name]
    uid = uuid.uuid4().hex
    doc = {'_id': uid, 'total_tweet': count_tweet}
    db.save(doc)

    return None

# Process of the slave processor
def slave_tweet_processor(comm):
    result = filter_data(comm)

    # send result to master processor
    comm.gather(result, root=0)
    
    return None

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    if rank == 0:
        # we are master
        master_tweet_processor(comm)
    else:
        # we are slave
        slave_tweet_processor(comm)

if __name__ == "__main__":
    main()
