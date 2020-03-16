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
    l = keepers[node_ip]
    l[0][port_no] = is_busy
    keepers[node_ip] = l
    lk.release()


def start_replicate(videos, lv, name, node_ip):
    lv.acquire()
    l = videos[name]
    l[0][node_ip] = False
    videos[name] = l
    lv.release()


def finish_replicate(videos, lv, name, node_ip):
    lv.acquire()
    l = videos[name]
    l[0][node_ip] = True
    videos[name] = l
    lv.release()


def set_alive(keepers, lk, node_ip, is_alive):
    lk.acquire()
    l = keepers[node_ip]
    l[-1] = is_alive
    keepers[node_ip] = l
    lk.release()


def set_busy(keepers, lk, node_ip, port_no, is_busy):
    lk.acquire()
    l = keepers[node_ip]
    l[0][str(port_no)] = is_busy
    keepers[node_ip] = l
    lk.release()
