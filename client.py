import zmq

import transfer

context = zmq.Context()
# transfer.download_from_server("file.mp4", context, "127.0.0.1", 5556)
transfer.upload_to_server("file.mp4", context, "127.0.0.1", 5556)
