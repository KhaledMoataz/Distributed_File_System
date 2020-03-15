import sys

import zmq

import transfer

master_ip = sys.argv[1]
context = zmq.Context()
socket = context.socket(zmq.REQ)
for i in range(len(sys.argv) - 2):
    socket.connect("tcp://{}:{}".format(master_ip, sys.argv[i + 2]))

command = input().split(" ")
while command[0] != "quit":
    operation = command[0]
    filename = command[1]
    if operation == "upload":
        socket.send_string("upload")
        response = socket.recv_json()
        if response is None:
            print("404: All ports are busy. Try again later :D")
        else:
            print("Uploading...")
            transfer.upload_to_server(filename, context, response[0], response[1])
    elif operation == "download":
        socket.send_string("download {}".format(filename))
        response = socket.recv_json()
        if response is None:
            print("File not found")
        elif len(response[0]) == 0:
            print("All ports are busy")
        else:
            print("Downloading...")
            transfer.download_from_servers(filename, context, response[0], response[1], response[2])
    command = input().split(" ")
# transfer.upload_to_server("file.mp4", context, "127.0.0.1", 5556)
