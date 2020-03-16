import zmq
import sys
import time


def establish_connection(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://*:{}'.format(port))
    return socket


def run(socket, ip):
    while True:
        socket.send_string("%s" % ip)
        time.sleep(1)


def init_datakeeper_heartbeat_process(ip, port):
    socket = establish_connection(ip, port)
    run(socket, ip)


if __name__ == "__main__":
    ip = sys.argv[1]
    port = sys.argv[2]
    init_datakeeper_heartbeat_process(ip, port)
