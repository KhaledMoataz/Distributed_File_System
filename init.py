from multiprocessing import Manager, Process, Lock

import master_core
import master_heartbeat
import replicates
import functions


def proc_replica():
    replica_factor = 2  # input
    replica_port = 6000  # hardcoded
    period = 5  # input

    return Process(target=replicates.init_replica_process,
                   args=(replica_factor, replica_port, period, videos, keepers, lv, lk))


def proc_heart_beats():
    # hardcoded
    init_keepers = \
        {
            "127.0.0.1": "5556"
        }

    functions.add_node(keepers, lk, "127.0.0.1", {"7777": False}, True)

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

        num_proc = 3  # input

        for i in range(num_proc):
            processes.append(Process(target=master_core.init_master_process, args=(5557 + i, videos, keepers, lv, lk)))

        for p in processes:
            p.start()

        for p in processes:
            p.join()
