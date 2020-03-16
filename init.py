from multiprocessing import Manager, Process, Lock

import master_core
import master_heartbeat
import replicates
import functions
import config


def proc_replica():
    replica_factor = config.replica_factor  # input
    replica_port = config.replica_port  # hardcoded
    period = config.replica_period  # input

    return Process(target=replicates.init_replica_process,
                   args=(replica_factor, replica_port, period, videos, keepers, lv, lk))


def proc_heart_beats():
    keeper_ports = {str(port): False for port in config.keeper_ports}
    for keeper_ip in config.keepers_ips:
        functions.add_node(keepers, lk, keeper_ip, keeper_ports, True)
    init_keepers = {ip: config.hearbeat_port for ip in config.keepers_ips}
    return Process(target=master_heartbeat.init_master_heartbeat_process, args=(init_keepers, keepers, lk))


if __name__ == '__main__':
    with Manager() as manager:

        processes = []

        videos = manager.dict()
        # 'file_name' : [[keeper_ids], user_id, 'file_path', size]
        keepers = manager.dict()
        # 'keeper_node_ip' : [{'port_numbers' : is_busy}, is_alive]

        lv = Lock()
        lk = Lock()

        processes.append(proc_heart_beats())
        processes.append(proc_replica())

        num_proc = config.number_of_masters  # input

        for i in range(num_proc):
            processes.append(Process(target=master_core.init_master_process,
                                     args=(config.master_start_port + i, videos, keepers, lv, lk)))

        for p in processes:
            p.start()

        for p in processes:
            p.join()
