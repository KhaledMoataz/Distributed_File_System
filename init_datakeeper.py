from multiprocessing import Process
from subprocess import check_output
import datakeeper_core
import datakeeper_heartbeat
from config import *

server_ip = str(check_output(['hostname', '-I'])).split()[0][2:]
Process(target=datakeeper_heartbeat.init_datakeeper_heartbeat_process,
        args=(server_ip, hearbeat_port)).start()
for i in range(data_keeper_core_number):
    Process(target=datakeeper_core.init_data_keeper_process,
            args=(server_ip, data_keeper_start_port + i, master_ip, master_ports, replica_port)).start()
