import zmq
import time
import functions

TIMEOUT = 2000


def establish_connection(keepers_dict):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVTIMEO, TIMEOUT)
    print("Connecting to data keepers")
    for ip in keepers_dict:
        socket.connect('tcp://{}:{}'.format(ip, keepers_dict[ip]))
    print("Connected to %d data keepers!" % len(keepers_dict))
    # Subscribe to all topics
    socket.subscribe("")
    return socket


def run(socket, keepers_dict, keepers, lk):
    last_heart_beat = {}
    last_check = time.time()
    for key in keepers_dict:
        last_heart_beat[key] = last_check
    while True:
        try:
            data_keeper_ip = socket.recv_string()
        except:
            print("All data keepers are dead!")
            continue

        last_heart_beat[data_keeper_ip] = time.time()
        curr_time = time.time()
        if curr_time - last_check >= 1:
            for key in keepers_dict:
                if curr_time - last_heart_beat[key] > 1:
                    functions.set_alive(keepers, lk, key, 0)
                else:
                    functions.set_alive(keepers, lk, key, 1)
            last_check = curr_time


def init_master_heartbeat_process(keepers_dict, keepers, lk):
    # keepers_dict: {'ip': 'port'}
    socket = establish_connection(keepers_dict)
    run(socket, keepers_dict, keepers, lk)
