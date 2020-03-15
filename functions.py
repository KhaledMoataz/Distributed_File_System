def add_video(videos, lv, name, keepers, user_id=None, path=None, size=None):
    lv.acquire()
    videos[name] = [keepers, user_id, path, size]
    lv.release()


def add_node(keepers, lk, node_ip, ports, is_alive):
    lk.acquire()
    keepers[node_ip] = [ports, is_alive]
    lk.release()


def add_port(keepers, lk, node_ip, port_no, is_busy):
    lk.acquire()
    l = keepers[node_id]
    l[0][port_no] = is_busy
    keepers[node_id] = l
    lk.release()


def replicate(videos, lv, name, node_ip):
    lv.acquire()
    l = videos[name]
    l[0].append(node_ip)
    videos[name] = l
    lv.release()


def set_alive(keepers, lk, node_id, is_alive):
    lk.acquire()
    l = keepers[node_ip]
    l[-1] = is_alive
    keepers[node_ip] = l
    lk.release()


def set_busy(keepers, lk, node_ip, port_no, is_busy):
    lk.acquire()
    l = keepers[node_ip]
    l[0][port_no] = is_busy
    keepers[node_ip] = l
    lk.release()


def get_keeper_ports(keepers, keeper_ip):
    all_ports = keepers[keeper_ip][0]
    available_ports = []
    for port, busy in all_ports.items():
        if not busy:
            available_ports.append(port)
    return available_ports
