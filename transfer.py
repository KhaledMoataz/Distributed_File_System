import zmq
import parse

CHUNK_SIZE = 1024 * 1024


def send_chunk(file, socket):
    chunk = file.read(CHUNK_SIZE)
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


def download_from_client(socket, request):
    filename = str(parse.parse("upload {}", request)[0])
    file = open(filename, "wb")
    has_next = True
    while has_next:
        socket.send_string("fetch")
        has_next = receive_chunk(file, socket)
    socket.send_string("done")
    file.close()


def upload_to_server(filename, context, ip, port):
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(ip, port))
    socket.send_string("upload {}".format(filename))
    file = open(filename, "rb")
    has_next = True
    while has_next:
        request = socket.recv_string()
        has_next = send_chunk(file, socket)
    file.close()


def download_from_server(filename, context, ip, port):
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(ip, port))
    file = open(filename, "wb")
    request = "fetch {} {}".format(filename, 0)
    socket.send_string(request)
    while True:
        if not receive_chunk(file, socket):
            break
        socket.send_string("fetch")
    file.close()


def upload_to_client(socket, request):
    parsed = parse.parse("fetch {} {}", request)
    filename = str(parsed[0])
    start = int(parsed[1])
    file = open(filename, "rb")
    file.seek(start)
    while True:
        if not send_chunk(file, socket):
            break
        request = socket.recv_string()
    file.close()
