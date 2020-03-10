#!/usr/bin/python3

import os
import subprocess
import sys

DB_VOL_NAME_BASE = "CS233_"
CONTAINER_NAME_BASE = "CS233_"

if len(sys.argv) != 2:
    print("Arg Error: Only take 1 argument, the number of agents in previous experiment")
    quit()

previous_num_agents = int(sys.argv[1])

#docker container stop cs223_postgres
cmd_arr = ["docker","container","stop",CONTAINER_NAME_BASE+"coordinator"]
print("Calling" + str(cmd_arr))
subprocess.call(cmd_arr)

cmd_arr = ["docker","volume","rm",DB_VOL_NAME_BASE+"coordinator_vol"]
print("Calling" + str(cmd_arr))
subprocess.call(cmd_arr)

for agent_id in range(previous_num_agents):
    cmd_arr = ["docker","container","stop",CONTAINER_NAME_BASE+"agent_"+str(agent_id)]
    print("Calling" + str(cmd_arr))
    subprocess.call(cmd_arr)

    cmd_arr = ["docker","volume","rm",DB_VOL_NAME_BASE+"agent_val_"+str(agent_id)]
    print("Calling" + str(cmd_arr))
    subprocess.call(cmd_arr)