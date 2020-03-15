import math
import threading

import parse
import zmq
import os

CHUNK_SIZE = 1024 * 1024

#---------------------Generic Send/Receive Functions---------------------
def send_chunk(file, socket, end):
    if end == -1:
        chunk_size = CHUNK_SIZE
    else:
        chunk_size = min(CHUNK_SIZE, end - file.tell())
    chunk = file.read(chunk_size)
    socket.send(chunk)
    if chunk:
        return True
    return False


def receive_chunk(file, socket):
    chunk = socket.recv()
    if len(chunk) == 0:
        return False
    file.write(chunk)
    return True


#---------------------Upload---------------------
#*********Client Master Side*********
# Client send upload request to master
# Master responds with a data keeper port to upload to
#*********Client Data Keeper Side*********
# Client calls (upload_to_server) to sends an upload request to data keeper
# Data keeper calls (download_from_client) for receiving and writing data
def download_from_client(socket, request):
    filename = str(parse.parse("upload {}", request)[0])
    file = open(filename, "wb")
    has_next = True
    while has_next:
        socket.send_string("")
        has_next = receive_chunk(file, socket)
    socket.send_string("")
    size = file.tell()
    file.close()
    return size


def upload_to_server(filename, context, ip, port):
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(ip, port))
    socket.send_string("upload {}".format(filename))
    file = open(filename, "rb")
    has_next = True
    while has_next:
        request = socket.recv_string()
        has_next = send_chunk(file, socket, -1)
    file.close()

#---------------------Download---------------------
#*********Client Master Side*********
# Client send download request to master
# Master responds with a data keeper port to download from
#*********Client Data Keeper Side*********
# Client calls (download_from_server(s)) to sends an upload request to data keeper
# Data keeper calls (upload_to_client) for sending data
def download_from_server(filename, filename_to_write, context, ip, port, start, end):
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(ip, port))
    file = open(filename_to_write, "wb")
    request = "fetch {} {} {}".format(filename, start, end)
    socket.send_string(request)
    while True:
        if not receive_chunk(file, socket):
            break
        socket.send_string("")
    file.close()


def upload_to_client(socket, request):
    parsed = parse.parse("fetch {} {} {}", request)
    filename = str(parsed[0])
    start = int(parsed[1])
    end = int(parsed[2])
    file = open(filename, "rb")
    file.seek(start)
    while True:
        if not send_chunk(file, socket, end):
            break
        request = socket.recv_string()
    file.close()


def async_download_from_server(filename, filename_to_write, context, ip, port, start, end):
    thread = threading.Thread(target=download_from_server,
                              args=(filename, filename_to_write, context, ip, port, start, end))
    thread.start()
    return thread


def download_from_servers(filename, context, ips, ports, size):
    file_part_size = math.floor(size / len(ips))
    filename_base = filename + "_merge_"
    threads = []
    for i in range(len(ips) - 1):
        threads.append(
            async_download_from_server(filename, filename_base + str(i), context, ips[i], ports[i], i * file_part_size,
                                       (i + 1) * file_part_size))
    i = len(ips) - 1
    threads.append(async_download_from_server(
        filename, filename_base + str(i), context, ips[i], ports[i], i * file_part_size, -1))

    file = open(filename + "_received", "wb")
    for i in range(len(ips)):
        threads[i].join()
        file_to_merge_name = filename_base + str(i)
        file_to_merge = open(file_to_merge_name, "rb")
        file.write(file_to_merge.read())
        file_to_merge.close()
        os.remove(file_to_merge_name)
    file.close()
