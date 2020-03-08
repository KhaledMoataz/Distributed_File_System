import numpy as np 
import sys
import zmq
import time

TIMEOUT = 2000

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.setsockopt(zmq.RCVTIMEO, TIMEOUT)

n = 0
print "Connecting to data keepers"
for i in range(1, len(sys.argv)):
    socket.connect("tcp://localhost:%s" % sys.argv[i])
    n += 1
print("Connected to %d data keepers!" % n)


# Subscribe to all topics
socket.subscribe("")

last_heart_beat = np.zeros(n, np.float)
is_alive = np.zeros(n, np.bool)
last_check = time.time()
while True:
    try:
        data_keeper_id = socket.recv()
    except:
        print("All data keepers are dead! Master will die now :(")
        break

    last_heart_beat[int(data_keeper_id)] = time.time()
    curr_time = time.time()
    if curr_time - last_check >= 1:
        for i in range(n):
            if curr_time - last_heart_beat[i] > 1:
                is_alive[i] = 0
            else:
                is_alive[i] = 1
        last_check = curr_time
        print(is_alive)
