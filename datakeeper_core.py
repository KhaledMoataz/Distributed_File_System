import sys

import zmq

import transfer

master_ip = sys.argv[1]
master_port = sys.argv[2]
server_port = sys.argv[3]

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://{}:{}".format("127.0.0.1", server_port))

while True:
    request = socket.recv_string()
    if request.startswith("upload"):
        transfer.download_from_client(socket, request)
    elif request.startswith("fetch"):
        transfer.upload_to_client(socket, request)
