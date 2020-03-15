def add_video(videos, lv, name, keepers, user_id=None, path=None, size=None):
    lv.acquire()
    videos[name] = [keepers, user_id, path, size]
    lv.release()


def add_node(keepers, lk, node_id, ip, ports, is_alive):
    lk.acquire()
    keepers[node_id] = [ip, ports, is_alive]
    lk.release()


def add_port(keepers, lk, node_id, port_no, is_busy):
    lk.acquire()
    l = keepers[node_id]
    l[1][port_no] = is_busy
    keepers[node_id] = l
    lk.release()


def replicate(videos, lv, name, node_id):
    lv.acquire()
    l = videos[name]
    l[0].append(node_id)
    videos[name] = l
    lv.release()


def set_alive(keepers, lk, node_id, is_alive):
    lk.acquire()
    l = keepers[node_id]
    l[-1] = is_alive
    keepers[node_id] = l
    lk.release()


def set_busy(keepers, lk, node_id, port_no, is_busy):
    lk.acquire()
    l = keepers[node_id]
    l[1][port_no] = is_busy
    keepers[node_id] = l
    lk.release()


def get_keeper_ports(keepers, keeper_id):
    all_ports = keepers[keeper_id][1]
    available_ports = []
    for port, busy in all_ports.items():
        if not busy:
            available_ports.append(port)
    return available_ports
