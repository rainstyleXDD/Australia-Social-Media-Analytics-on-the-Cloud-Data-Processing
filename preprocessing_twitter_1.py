"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

# import libraries
import json
from mpi4py import MPI

# constant
FILE_NUM = 10
LGA_FILE_PATH = './sudo_data/lga.json'

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Opening lga JSON file
    with open(LGA_FILE_PATH, 'r') as lga:

        # returns JSON object as a dictionary
        lga_data = json.load(lga)

    # preprocess each file
    for i in range(FILE_NUM):
        file_path_r = './raw_data/tweetsWithLocation' + str(rank) + '_' + str(i) + '.json'
        file_path_w = './curated_data/tweetsProcessed' + str(rank) + '_' + str(i) + '.json'

        # check whether the file exist
        try:
            file = open(file_path_r, 'rb')
        except IOError:
            break
        else:
            file.close()


        with open(file_path_r, 'rb') as t, open(file_path_w, 'w') as outfile:

            # returns JSON object as a dictionary
            tweet_data = json.load(t)
            outfile.write('[\n')

            # initialise the dictionary
            tweet_dic = {
                'id': 0,
                'value': 0,
                'author_id': 0,
                'created_at': 0,
                'text': 0,
                'sentiment': 0,
                'full_name': 0,
                'state': 0
            }

            # start count the number of tweet
            count = 0

            for tweet in tweet_data:

                # filter the non-english tweets
                if tweet['doc']['data']['lang'] == 'en':

                    # find the place name of the tweet
                    try:
                        tweet['doc']['includes']['places'][0]['full_name']
                    except TypeError:
                        place_name = tweet['doc']['includes'][0]['name'].split(',')[0].lower()
                    else:
                        place_name = tweet['doc']['includes']['places'][0]['full_name'].split(',')[0].lower()

                    # filter the place with unclear location
                    if place_name in lga_data:
                        
                        # formate the JSON file
                        if count != 0:
                            outfile.write(',\n')
                            
                        # save the useful information
                        tweet_dic['id'] = tweet['id']
                        tweet_dic['value'] = tweet['value']
                        tweet_dic['author_id'] = tweet['doc']['data']['author_id']
                        tweet_dic['created_at'] = tweet['doc']['data']['created_at']
                        tweet_dic['text'] = tweet['doc']['data']['text']
                        tweet_dic['sentiment'] = tweet['doc']['data']['sentiment']
                        tweet_dic['full_name'] = lga_data[place_name]['fullname']
                        tweet_dic['state'] = lga_data[place_name]['state']

                        # change the dictionary to a json object
                        json_object = json.dumps(tweet_dic, indent=2)
                        outfile.write(json_object)
                        count += 1
                    
            # close the formated JSON file
            outfile.write('\n]')
            print(count)

    return None

if __name__ == "__main__":
    main()