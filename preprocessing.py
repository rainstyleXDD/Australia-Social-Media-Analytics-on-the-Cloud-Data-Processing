import json
from mpi4py import MPI

FILE_NUM = 10
state_abb = {'1': 'NSW',
            '2': 'VIC',
            '3': 'QLD',
            '4': 'SA',
            '5': 'WA',
            '6': 'TAS',
            '7': 'NT',
            '8': 'ACT',
            '9': 'OT'}

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Opening sal JSON file
    with open('sal.json', 'r') as sal:

        # returns JSON object as a dictionary
        sal_data = json.load(sal)


    for i in range(FILE_NUM):
        file_path_r = './raw_data/tweetsWithLocation' + str(rank) + '_' + str(i) + '.json'
        file_path_w = './curated_data/tweetsProcessed' + str(rank) + '_' + str(i) + '.json'

        try:
            file = open(file_path_r, 'rb')
        except IOError:
            print(f'not find {rank}-{i}')
            break
        else:
            file.close()


        with open(file_path_r, 'rb') as t, open(file_path_w, 'w') as outfile:

            # returns JSON object as a dictionary
            tweet_data = json.load(t)
            outfile.write('[\n')
            tweet_dic = {
                'id': 0,
                'value': 0,
                'author_id': 0,
                'created_at': 0,
                'text': 0,
                'sentiment': 0,
                'full_name': 0,
                'state': 0,
                'bbox': 0
            }
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
                    if place_name in sal_data:
                        
                        if count != 0:
                            outfile.write(',\n')
                            
                        # save the useful information
                        tweet_dic['id'] = tweet['id']
                        tweet_dic['value'] = tweet['value']
                        tweet_dic['author_id'] = tweet['doc']['data']['author_id']
                        tweet_dic['created_at'] = tweet['doc']['data']['created_at']
                        tweet_dic['text'] = tweet['doc']['data']['text']
                        tweet_dic['sentiment'] = tweet['doc']['data']['sentiment']
                        tweet_dic['full_name'] = place_name
                        ste = sal_data[place_name]['ste']
                        tweet_dic['state'] = state_abb[ste]

                        try:
                            tweet['doc']['includes']['places'][0]['geo']['bbox']
                        except TypeError:
                            box = tweet['doc']['includes'][0]['bounding_box']
                            box['centroid'] = tweet['doc']['includes'][0]['centroid']
                            tweet_dic['bbox'] = box
                        else:
                            tweet_dic['bbox'] = tweet['doc']['includes']['places'][0]['geo']['bbox']
                        

                        # change the dictionary to a json object
                        json_object = json.dumps(tweet_dic, indent=4)
                        outfile.write(json_object)
                        count += 1
                    

            outfile.write('\n]')
    return

if __name__ == "__main__":
    main()