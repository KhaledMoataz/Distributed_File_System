import zmq
import os

import transfer

context = zmq.Context()
file = open("file.mp4", "rb")
file.seek(0, os.SEEK_END)
transfer.download_from_servers("file.mp4", context, ["127.0.0.1", "127.0.0.1"], [5556, 5557], file.tell())
# transfer.upload_to_server("file.mp4", context, "127.0.0.1", 5556)
