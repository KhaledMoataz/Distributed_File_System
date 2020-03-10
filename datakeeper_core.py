import sys
import zmq
import transfer
import parse

master_ip = sys.argv[1]
master_port = sys.argv[2]
server_port = sys.argv[3]
id = sys.argv[4]

print(server_port)
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://{}:{}".format("127.0.0.1", server_port))

socket2 = context.socket(zmq.REQ)
socket2.connect('tcp://{}:{}'.format('127.0.0.1',master_port))

while True:
    request = socket.recv_string()
    if request.startswith("upload"):
        transfer.download_from_client(socket, request)
    elif request.startswith("fetch"):
        transfer.upload_to_client(socket, request)
    elif request.startswith("replica"):
        parsed_req = parse.parse('replica {} {} {}', request)
        file_name = str(parsed_req[0])
        server_ip = str(parsed_req[1])
        port = str(parsed_req[2])
        socket.send_string('replicating file: {}.....'.format(file_name))
        transfer.download_from_server(file_name, context, server_ip, port)
        socket2.send_string('{} {} {} {}'.format(file_name, id, server_port, port))
        response = socket2.recv_string()
        print(response)
