import sys
import threading

import parse
import zmq

import functions


class Replicas:

    def __init__(self, replica_factor, replica_port, videos, keepers, lv, lk):
        self.replica_factor = replica_factor
        self.replica_port = replica_port
        self.videos = videos
        self.keepers = keepers
        self.lv = lv
        self.lk = lk

    # dummy look-up tables...
    # this look up table holds the Ip of each data keeper and their ports indicating available/ not available ports...
    replica_port = sys.argv[1]

    data_keeper_info = {
        '1': {'ip': '127.0.0.1', 'ports': [5556]},
        '2': {'ip': '127.0.0.1', 'ports': [5557]},
        '3': {'ip': '127.0.0.1', 'ports': [5558]},
        '4': {'ip': '127.0.0.1', 'ports': [5559]}
    }

    is_port_available = {
        '5556': True,
        '5557': True,
        '5558': True,
        '5559': True
    }

    files = {
        'a.mp4': [1],
        'b.mp4': [1, 2, 3],
        'c.mp4': [4, 2]
    }

    def get_available_port(self, id):
        r_port = -1  # keeps -1 if no port available....
        for port in self.keepers[id][1]:
            if self.keepers[id][1][port]:
                r_port = port
                break
        return r_port

    def get_source(self, file):
        for machine in self.videos[file][0]:
            port = self.get_available_port(machine)
            if port != -1:
                return machine, port
        return '', -1  # no source available....

    def get_destination(self, file):
        for id in self.keepers.keys():
            if int(id) in self.videos[file]:
                continue
            port = self.get_available_port(id)
            if port != -1:
                return id, port
        return '', -1  # no available destination....

    def activate(self, ports):
        for machine, port in ports:
            functions.set_busy(self.keepers, self.lk, machine, port, False)

    def deactivate(self, ports):
        for machine, port in ports:
            functions.set_busy(self.keepers, self.lk, machine, port, True)

    def inform_replicated(self, replica_port):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind('tcp://127.0.0.1:{}'.format(replica_port))
        while True:
            request = socket.recv_string()
            parsed_req = parse.parse('{} {} {} {}', request)            # TODO: recieve id of sender to use in activate
            file_name = str(parsed_req[0])
            id_machine = int(parsed_req[1])
            src_port = str(parsed_req[2])
            dst_port = str(parsed_req[3])
            # update look_up table...
            functions.replicate(self.videos, self.lv, file_name, id_machine)
            socket.send_string('informed that file {} replicated in datakeeper {}'.format(file_name, id_machine))
            self.activate([(, src_port), (id_machine, dst_port)])       # TODO: need id of sending machine

    def manage_replications(self):
        rep_thread = threading.Thread(target=self.inform_replicated, args=[self.replica_port])
        rep_thread.start()

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        while True:
            for file in self.videos.keys():
                if len(self.videos[file][0]) < self.replica_factor:
                    src_id, src_port = self.get_source(str(file))
                    dst_id, dst_port = self.get_destination(str(file))
                    src_ip = self.keepers[src_id][0]
                    dst_ip = self.keepers[dst_id][0]
                    if src_port == -1:
                        print('no source machine available...')
                    elif dst_port == -1:
                        print('no destination machine available...')
                    else:
                        socket.connect('tcp://{}:{}'.format(dst_ip, dst_port))
                        request = 'replica {} {} {}'.format(file, src_ip, src_port)
                        socket.send_string(request)
                        response = socket.recv_string()
                        self.deactivate([(src_id, src_port), (dst_id, dst_port)])
                        print(response)
                        socket.disconnect('tcp://{}:{}'.format(dst_ip, dst_port))
            print('all files have n replicas, yeah it is done :v')


manage_replications(3, replica_port)
