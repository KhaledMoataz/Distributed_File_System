import zmq
import sys
import time

def establish_connection(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://{}:{}'.format(ip, port))
    return socket


def run(socket, ip):
    while True:
        socket.send("%s" % ip)
        time.sleep(1)


def init_datakeeper_heartbeat_process(ip, port):
    socket = establish_connection(ip, port)
    run(socket, ip)