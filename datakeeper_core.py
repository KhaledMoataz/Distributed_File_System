import parse
import zmq

import transfer


# As a data keeper what I need to know:
# my ip in the network, my opened port number -> bind, so that anyone can connect (I'm the server)
# the master ip, master port -> connect to it, to inform the master that the replication is completed (I'm the client)
class DataKeeper:

    def __init__(self, server_ip, server_port):
        self.context = None
        self.socket = None
        self.replicas_master_socket = None
        self.master_core_socket = None
        self.server_ip = server_ip
        self.server_port = server_port

    def establish_connection(self, master_ip, master_ports, replica_port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://{}:{}".format(self.server_ip, self.server_port))
        self.replicas_master_socket = self.context.socket(zmq.REQ)
        self.replicas_master_socket.connect("tcp://{}:{}".format(master_ip, replica_port))
        self.master_core_socket = self.context.socket(zmq.REQ)
        for master_port in master_ports:
            self.master_core_socket.connect("tcp://{}:{}".format(master_ip, master_port))

    def run(self):
        while True:
            request = self.socket.recv_string()
            if request.startswith("upload"):  # upload user_id filename
                parsed = parse.parse("upload {} {}", request)
                user_id = int(parsed[0])
                filename = str(parsed[1])
                size = transfer.download_from_client(self.socket, filename)
                self.master_core_socket.send_string(
                    "upload success: {} {} {} {} {} {}".format(filename, self.server_ip, self.server_port, user_id, "/",
                                                               size))
            elif request.startswith("fetch"):
                transfer.upload_to_client(self.socket, request)
                self.master_core_socket.send_string("download success: {} {}".format(self.server_ip, self.server_port))
            elif request.startswith("replica"):
                parsed_req = parse.parse('replica {} {} {}', request)
                file_name = str(parsed_req[0])
                source_ip = str(parsed_req[1])
                source_port = str(parsed_req[2])
                self.socket.send_string('replicating file: {}.....'.format(file_name))
                transfer.download_from_server(file_name, self.context, source_ip, source_port)
                self.replicas_master_socket.send_string(
                    '{} {} {} {} {}'.format(file_name, source_ip, source_port, self.server_ip, self.server_port))
                response = self.replicas_master_socket.recv_string()
                print(response)


def init_data_keeper_process(server_ip, server_port, master_ip, master_ports, replicas_master_port):
    data_keeper = DataKeeper(server_ip, server_port)
    data_keeper.establish_connection(master_ip, master_ports, replicas_master_port)
    data_keeper.run()
