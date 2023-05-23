# import libraries
import json
from mpi4py import MPI
from datetime import datetime
import pytz
import uuid
import couchdb
from collections import Counter

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


# Filter the tweet which not relavant to the scenario
def filter_data():
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

    # process all files
    for i in range(file_num):
        file_path_r = './curated_data/tweetsProcessed' + str(rank) + '_' + str(i) + '.json'
        print(rank, "reading", i)
        with open(file_path_r, 'r') as file:

            # returns JSON object as a dictionary
            data = json.load(file)
            
            for tweet in data:

                # initialise the dictionary
                

                

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
                    lga_tweet_latenight[tweet["full_name"]] += 1
                    
                
                # check whether the tweet match the second scenario
                for keyword in CLIMATE_KEYWORD:
                    if (keyword in tweet['value']['tokens']) or (keyword in tweet['value']['tags']):
                        lga_tweet_climate[tweet["full_name"]] += 1
                        break
                
                lga_tweet[tweet["full_name"]] += 1

    # aggregate and print out the formated results
    if rank == 0:
        
        # collect the data
        for i in range(1, size):
            lga_tweet += comm.recv(source=i, tag=1)
            lga_tweet_climate += comm.recv(source=i, tag=2)
            lga_tweet_latenight = comm.recv(source=i, tag=3)

        print(sum(lga_tweet.values()))
        print(sum(lga_tweet_climate.values()))
        print(sum(lga_tweet_latenight.values()))
        with open('./processed_data/tweetPlaceCount' + '.json', 'w') as outfile:
            json_lga_tweet = json.dumps(dict(lga_tweet))
            outfile.write(json_lga_tweet)
        with open('./processed_data/tweetClimateCount' + '.json', 'w') as outfile:
            json_lga_tweet_climate = json.dumps(dict(lga_tweet_climate))
            outfile.write(json_lga_tweet_climate)

        with open('./processed_data/tweetNightCount' + '.json', 'w') as outfile:
            json_lga_tweet_latenight = json.dumps(dict(lga_tweet_latenight))
            outfile.write(json_lga_tweet_latenight)
            

    else:
        comm.send(lga_tweet, 0, tag=1)
        comm.send(lga_tweet_climate, 0, tag=2)
        comm.send(lga_tweet_latenight, 0, tag=3)


    return 

def main():
    filter_data()

if __name__ == "__main__":
    main()
