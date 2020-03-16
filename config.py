config_file = open('config.txt', 'r')
lines = config_file.readlines()
config_file.close()
replica_factor = int(lines[1])
replica_port = int(lines[3])
replica_period = int(lines[5])
hearbeat_port = int(lines[7])
master_ip = lines[9].strip()
number_of_masters = int(lines[11])
master_start_port = int(lines[13])
data_keeper_core_number = int(lines[15])
data_keeper_start_port = int(lines[17])
data_keepers_number = int(lines[19])

master_ports = [master_start_port + i for i in range(number_of_masters)]
keeper_ports = [data_keeper_start_port + i for i in range(data_keeper_core_number)]
keepers_ips = [lines[i].strip() for i in range(20, 20 + data_keepers_number, 1)]
