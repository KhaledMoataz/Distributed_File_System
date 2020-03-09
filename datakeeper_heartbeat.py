import zmq
import sys
import time

port = sys.argv[1]
id = int(sys.argv[2])

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:%s" % port)

while True:
    socket.send("%d" % id)
    time.sleep(1)
