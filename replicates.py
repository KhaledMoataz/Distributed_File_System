import zmq
import sys
import numpy as np
import transfer
import parse
import threading

# dummy look-up tables...
# this look up table holds the Ip of each data keeper and their ports indicating available/ not available ports...
replica_port = sys.argv[1]

data_keeper_info = {
    '1' : {'ip' : '127.0.0.1' , 'ports' : [5556]},
    '2' : {'ip' : '127.0.0.1' , 'ports' : [5557]},
    '3' : {'ip' : '127.0.0.1' , 'ports' : [5558]},
    '4' : {'ip' : '127.0.0.1' , 'ports' : [5559]}
}

is_port_available = {
    '5556' : True,
    '5557' : True,
    '5558' : True,
    '5559' : True
}

files = {
    'a.mp4' : [1],
    'b.mp4' : [1,2,3],
    'c.mp4' : [4,2]
}

def get_available_port(id):
    r_port = -1 # keeps -1 if no port available....
    for port in data_keeper_info[str(id)]['ports']:
        if is_port_available[str(port)]:
            r_port = port
            break
    return r_port

def get_source_ip_port(file):
    for machine in files[file]:
        port = get_available_port(machine)
        if port != -1:
            return data_keeper_info[str(machine)]['ip'], port
    return '', -1 # no source available....


def get_destination_ip_port(file):
    for id in data_keeper_info.keys():
        if int(id) in files[file]:
            continue
        port = get_available_port(id)
        if port != -1:
            return data_keeper_info[str(id)]['ip'], port
    return '', -1 # no available destination....

def activate(ports):
    for port in ports:
        is_port_available[str(port)] = True

def deactivate(ports):
    for port in ports:
        is_port_available[str(port)] = False



def inform_replicated(replica_port):

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:{}'.format(replica_port))
    while True:
        request = socket.recv_string()
        parsed_req = parse.parse('{} {} {} {}',request)
        file_name = str(parsed_req[0])
        id_machine = int(parsed_req[1])
        src_port = str(parsed_req[2])
        dst_port = str(parsed_req[3])
        #update look_up table...
        files[file_name].append(id_machine)
        socket.send_string('informed that file {} replicated in datakeeper {}'.format(file_name, id_machine))
        activate([src_port, dst_port])





def manage_replications(n, replica_port):

    rep_thread = threading.Thread(target=inform_replicated, args=[replica_port])
    rep_thread.start()

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    while True:
        sure_all_done = 1
        for file in files.keys():
            if len(files[file]) < n:
                sure_all_done = 0
                src_ip, src_port = get_source_ip_port(str(file))
                dst_ip, dst_port = get_destination_ip_port(str(file))
                if src_port == -1:
                    print('no source machine available...')
                elif dst_port == -1:
                    print('no destination machine available...')
                else:
                    socket.connect('tcp://{}:{}'.format(dst_ip, dst_port))
                    request = 'replica {} {} {}'.format(file, src_ip, src_port)
                    socket.send_string(request)
                    response = socket.recv_string()
                    deactivate([src_port, dst_port])
                    print(response)
                    socket.disconnect('tcp://{}:{}'.format(dst_ip, dst_port))
            if(sure_all_done):
                print('all files have n replicas, yeah it is done :v')






manage_replications(3, replica_port)
