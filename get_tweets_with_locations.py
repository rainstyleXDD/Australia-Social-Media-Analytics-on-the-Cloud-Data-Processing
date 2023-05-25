"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

# import libraries
from mpi4py import MPI
import time
import os

# constants
PATH = "./twitter-huge.json/mnt/ext100/twitter-huge.json"

def main():
    """ The program starts executing from here. """

    # Sets up MPI execution environment
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    pickle_size = 100000

    t1_start = time.perf_counter()

    with open(PATH, "r") as f:

        # get size of the file and partition the file
        tweet_file_size = os.path.getsize(PATH)
        chunk_size = tweet_file_size // size
        start = rank * chunk_size
        end = (rank + 1) * chunk_size if rank != size - 1 else tweet_file_size 

        count = 0
        total_count = 0
        pickle_count = 0

        # find end _id
        f.seek(end, 0)
        
        end_id = 'END'
        for line in f:
            if ('{"id":"' in line) and ('","key"' in line):
                end_id = line.split('{"id":"')[1].split('","key"')[0]
                break

        f.seek(start, 0)        # Moves read/write head to the correct position
        f_output = open('tweetsWithLocation' + str(rank)+ '_' +str(pickle_count) + '.json', 'w')
        f_output.write("[\n")
        for line in f:
            
            if (line.startswith('{"id":')):
                # check end partition
                id_temp = line.split('{"id":"')[1].split('","key"')[0]
                if id_temp == end_id:
                    break
                
                # store json with locations
                if '"full_name"' in line:
                    count += 1
                    f_output.write(line)

                    # close and open write new file when tweets read exceeds pickle size
                    if count >= pickle_size:
                        f_output.write(']\n')
                        f_output.close()
                        
                        pickle_count += 1
                        total_count += count
                        count = 0
                        f_output = open('tweetsWithLocation' + str(rank)+ '_' +str(pickle_count) + '.json', 'w')
                        f_output.write("[\n")
        total_count += count
        f_output.write(']\n')
        f_output.close()

    # clean the ',' at the end of file for valid json  
    for i in range(pickle_count+1):
        with open('tweetsWithLocation' + str(rank)+ '_' +str(i) + '.json', 'r') as file:
        # read a list of lines into data
            data = file.readlines()
            
            if data[-2][-2] == ',':
                data[-2] = data[-2][:-2] + '\n'

        # and write everything back
        with open('tweetsWithLocation' + str(rank)+ '_' +str(i) + '.json', 'w') as file:
            file.writelines(data)

        print(rank, total_count)
        t1_end = time.perf_counter()
        print(rank, t1_end - t1_start)

    return 
        
if __name__ == "__main__":
    main()