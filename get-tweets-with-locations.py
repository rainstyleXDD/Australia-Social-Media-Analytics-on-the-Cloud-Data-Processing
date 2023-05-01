from mpi4py import MPI
import time
import os

def main():
    """ The program starts executing from here. """

    # Sets up MPI execution environment
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    t1_start = time.perf_counter()
    path = "./twitter-huge.json/mnt/ext100/twitter-huge.json"
    # with open('sal.json', "rb") as f:
    #     sal = json.load(f)

    with open(path, "r") as f, open('tweetsWithLocation' + str(rank) + '.json', 'w') as f_output:
        f_output.write("[\n")

        # get size of the file and partition the file
        tweet_file_size = os.path.getsize(path)
        chunk_size = tweet_file_size // size
        start = rank * chunk_size
        end = (rank + 1) * chunk_size if rank != size - 1 else tweet_file_size 

        #print(rank, start, end)
        count = 0
        # find end _id
        f.seek(end, 0)
        
        end_id = 'END'
        for line in f:
            if ('{"id":"' in line) and ('","key"' in line):
                end_id = line.split('{"id":"')[1].split('","key"')[0]
                break
        
        #print(rank, end_id)

        f.seek(start, 0)        # Moves read/write head to the correct position

        for line in f:
            count += 1
            
            if (line.startswith('{"id":')):
                # check end partition
                id_temp = line.split('{"id":"')[1].split('","key"')[0]
                if id_temp == end_id:
                    break
                
                # store json with locations
                if '"full_name"' in line:
                    f_output.write(line)
            

        f_output.write(']\n')
        print(rank, count)
       
    

    with open('tweetsWithLocation' + str(rank) + '.json', 'r') as file:
    # read a list of lines into data
        data = file.readlines()

        # now change the 2nd line, note that you have to add a newline
        if data[-2][-2] == ',':
            data[-2] = data[-2][:-2] + '\n'

    # and write everything back
    with open('tweetsWithLocation' + str(rank) + '.json', 'w') as file:
        file.writelines(data)
    
    return 
            
        
if __name__ == "__main__":
    main()