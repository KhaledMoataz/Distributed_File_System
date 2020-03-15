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

    def get_available_port(self, ip):
        r_port = -1  # keeps -1 if no port available....
        for port in self.keepers[ip][1]:
            if self.keepers[ip][1][port]:
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
        for ip in self.keepers.keys():
            if int(ip) in self.videos[file]:
                continue
            port = self.get_available_port(ip)
            if port != -1:
                return ip, port
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
            parsed_req = parse.parse('{} {} {} {} {}', request)
            file_name = str(parsed_req[0])
            src_ip = int(parsed_req[1])
            src_port = str(parsed_req[2])
            dst_ip = int(parsed_req[3])
            dst_port = str(parsed_req[4])
            # update look_up table...
            functions.replicate(self.videos, self.lv, file_name, dst_ip)
            socket.send_string('informed that file {} replicated in datakeeper {}'.format(file_name, dst_ip))
            self.activate([(src_ip, src_port), (dst_ip, dst_port)])

    def manage_replications(self):
        rep_thread = threading.Thread(target=self.inform_replicated, args=[self.replica_port])
        rep_thread.start()

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        while True:
            for file in self.videos.keys():
                if len(self.videos[file][0]) < self.replica_factor:
                    src_ip, src_port = self.get_source(str(file))
                    dst_ip, dst_port = self.get_destination(str(file))
                    if src_port == -1:
                        print('no source machine available...')
                    elif dst_port == -1:
                        print('no destination machine available...')
                    else:
                        socket.connect('tcp://{}:{}'.format(dst_ip, dst_port))
                        request = 'replica {} {} {}'.format(file, src_ip, src_port)
                        socket.send_string(request)
                        response = socket.recv_string()
                        self.deactivate([(src_ip, src_port), (dst_ip, dst_port)])
                        print(response)
                        socket.disconnect('tcp://{}:{}'.format(dst_ip, dst_port))
            print('all files have n replicas, yeah it is done :v')


manage_replications(3, replica_port)
