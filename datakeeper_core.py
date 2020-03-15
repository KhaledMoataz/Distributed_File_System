import zmq
import transfer
import parse

#As a data keeper what I need to know:
# my ip in the network, my opened port number -> bind, so that anyone can connect (I'm the server)
# the master ip, master port -> connect to it, to send replication confirmation requests (I'm the client)
class DataKeeper:

    def __init__(self, server_ip, server_port):
        self.context = None
        self.socket = None
        self.replicas_master_socket = None
        self.server_ip = server_ip
        self.server_port = server_port

    def establish_connection(self, replicas_master_ip, replicas_master_port):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://{}:{}".format(self.server_ip, self.server_port))

        replicas_master_socket = context.socket(zmq.REQ)
        replicas_master_socket.connect('tcp://{}:{}'.format(replicas_master_ip, replicas_master_port))

    def run(self):
        while True:
            request = self.socket.recv_string()
            if request.startswith("upload"):
                size = transfer.download_from_client(self.socket, request)
            elif request.startswith("fetch"):
                transfer.upload_to_client(self.socket, request)
            elif request.startswith("replica"):
                parsed_req = parse.parse('replica {} {} {}', request)
                file_name = str(parsed_req[0])
                source_ip = str(parsed_req[1])
                source_port = str(parsed_req[2])
                self.socket.send_string('replicating file: {}.....'.format(file_name))
                transfer.download_from_server(file_name, self.context, source_ip, source_port)
                self.replicas_master_socket.send_string('{} {} {} {} {}'.format(file_name, source_ip, source_port, self.server_ip, self.server_port))
                response = self.replicas_master_socket.recv_string()
                print(response)


def init_data_keeper_process(server_ip, server_port, replicas_master_ip, replicas_master_port):
    data_keeper = DataKeeper(server_ip, server_port)
    data_keeper.establish_connection(replicas_master_ip, replicas_master_port)
    data_keeper.run()
