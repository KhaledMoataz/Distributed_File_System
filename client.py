import sys

import zmq

import transfer

user_id = sys.argv[1]
master_ip = sys.argv[2]
context = zmq.Context()
socket = context.socket(zmq.REQ)
for i in range(3, len(sys.argv), 1):
    socket.connect("tcp://{}:{}".format(master_ip, sys.argv[i]))

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
            transfer.upload_to_server(filename, user_id, context, response[0], response[1])
            print('"{}" uploaded successfully'.format(filename))
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
            print('"{}" downloaded successfully'.format(filename))
    command = input().split(" ")
# transfer.upload_to_server("file.mp4", context, "127.0.0.1", 5556)
