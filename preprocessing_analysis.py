# import libraries
import json
from mpi4py import MPI
from datetime import datetime
import pytz
import uuid
import couchdb
from collections import Counter, defaultdict
from constant import const
# constant



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
    #CLOCK = {hour:0 for hour in range(24)}
    lga_tweet_time = defaultdict(Counter)

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
                
                # check whether the tweet match the first scenario
                if local_time.hour >= 23 or local_time.hour <= 5:
                    lga_tweet_latenight[tweet["full_name"]] += 1
                    
                
                # check whether the tweet match the second scenario
                for keyword in const.CLIMATE_KEYWORD:
                    if (keyword in tweet['value']['tokens']) or (keyword in tweet['value']['tags']):
                        lga_tweet_climate[tweet["full_name"]] += 1
                        break

                lga_tweet_time[tweet["full_name"]][local_time.hour//1] +=1
                lga_tweet[tweet["full_name"]] += 1

    # aggregate and print out the formated results
    if rank == 0:
        lga_tweet_time_list = []
        # collect the data
        for i in range(1, size):
            lga_tweet += comm.recv(source=i, tag=1)
            lga_tweet_climate += comm.recv(source=i, tag=2)
            lga_tweet_latenight += comm.recv(source=i, tag=3)
            lga_tweet_time_list.append(comm.recv(source=i, tag=4))

        for dic in lga_tweet_time_list:
            for clock in dic:
                lga_tweet_time[clock] += dic[clock]

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
            
        with open('./processed_data/tweetTimeCount' + '.json', 'w') as outfile:
            json_lga_tweet_time = json.dumps(dict(lga_tweet_time))
            outfile.write(json_lga_tweet_time)

    else:
        comm.send(lga_tweet, 0, tag=1)
        comm.send(lga_tweet_climate, 0, tag=2)
        comm.send(lga_tweet_latenight, 0, tag=3)
        comm.send(lga_tweet_time, 0, tag=4)


    return 



if __name__ == "__main__":
    main()