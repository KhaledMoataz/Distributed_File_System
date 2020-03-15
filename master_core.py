# receive request from the client (filename) -> respond with a list of ports: download filename
# upload request from client: upload
# dataKeeper sends upload success upload message: (upload success: filename dataKeeper_id user_id file_path size)
# dataKeeper sends download success message: (download success: ip port)
import parse
import zmq

from functions import *


class Master:

    def __init__(self, port, videos, keepers, lv, lk):
        self.videos = videos
        self.keepers = keepers
        self.lv = lv
        self.lk = lk
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://{}:{}".format("127.0.0.1", port))

    def run(self):
        while True:
            request = self.socket.recv_string()
            # download filename
            if request.startswith("download"):
                filename = str(parse.parse("download {}", request)[0])
                keeper_data = self.videos.get(filename)
                if keeper_data is None:
                    self.socket.send_json(None)
                    continue
                keeper_ips = keeper_data[0]
                file_size = self.videos[filename][3]
                ips = []
                ports = []
                for ip in keeper_ips:
                    for port, busy in self.keepers[ip][0].items():
                        if not busy:
                            ips.append(ip)
                            ports.append(port)
                            set_busy(self.keepers, self.lk, ip, port, True)
                            break
                self.socket.send_json((ips, ports, file_size))

            elif request.startswith("upload"):
                keepers_used_storage = {}
                for file, data in self.videos.items():
                    for ip in data[0]:
                        keepers_used_storage[ip] += data[3]
                keepers_ips = [ip for ip, used_storage in
                               sorted(keepers_used_storage.items(), key=lambda item: item[1])]
                chosen_ip = None
                chosen_port = None
                for ip in keepers_ips:
                    for port, busy in self.keepers[ip][0].items():
                        if not busy:
                            chosen_ip = ip
                            chosen_port = port
                            break
                    if chosen_ip is not None:
                        break
                if chosen_ip is not None:
                    self.socket.send_json((chosen_ip, chosen_port))
                    set_busy(self.keepers, self.lk, chosen_ip, chosen_port, True)
                else:
                    self.socket.send_json(None)

            elif request.startswith("upload success"):
                parsed = parse.parse("success: {} {} {} {} {} {}", request)
                filename = str(parsed[0])
                data_keeper_ip = int(parsed[1])
                data_keeper_port = int(parsed[2])
                user_id = int(parsed[3])
                file_path = str(parsed[4])
                size = int(parsed[5])
                add_video(self.videos, self.lv, filename, [data_keeper_ip], user_id, file_path, size)
                set_busy(self.keepers, self.lk, data_keeper_ip, data_keeper_port, False)
                self.socket.send_string("OK")

            elif request.startswith("download success"):
                parsed = parse.parse("download success: {} {}", request)
                data_keeper_ip = int(parsed[0])
                data_keeper_port = int(parsed[1])
                set_busy(self.keepers, self.lk, data_keeper_ip, data_keeper_port, False)
                pass


def init_master_process(port, videos, keepers, lv, lk):
    master = Master(port, videos, keepers, lv, lk)
    master.run()
