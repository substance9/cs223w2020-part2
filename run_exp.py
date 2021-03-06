#!/usr/bin/python3
import os
import subprocess
import time
import pathlib
from datetime import datetime
from signal import signal, SIGINT
from sys import exit

PROJECT_DIR_PATH = "/home/guoxi/Workspace/cs223w2020-part2"

COORDINATOR_DB_PORT = 10009
AGENT_DB_PORTS_STARTS_AT = 10010
AGENT_APP_PORTS_STARTS_AT = 20010
DB_VOL_NAME_BASE = "CS233_"
CONTAINER_NAME_BASE = "CS233_"

proc_list = []

def exit_handler(signal_received, frame):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    for proc in proc_list:
        proc.kill()
    exit(0)

signal(SIGINT, exit_handler)

def call_cmd(cmd_arr):
    print("Calling" + str(cmd_arr))
    subprocess.call(cmd_arr)

def call_cmd_no_blocking(cmd_arr, output_file):
    print("Calling" + str(cmd_arr))
    proc = subprocess.Popen(cmd_arr, stdout=output_file)
    return proc

def create_db_vols(num_agents):
    coordinator_vol_name = DB_VOL_NAME_BASE+"coordinator_vol"
    call_cmd(["docker","volume","create",coordinator_vol_name])
    for agent_id in range(num_agents):
        agent_vol_name = DB_VOL_NAME_BASE+"agent_val_"+str(agent_id)
        call_cmd(["docker","volume","create",agent_vol_name])

def start_db_containers(num_agents):
    #example: docker run --name cs223_postgres --rm --volume=$(pwd)/postgres_data:/var/lib/postgresql/data -p 10020:5432 --shm-size=8G -e POSTGRES_PASSWORD=password -d postgres:12.1 -N 1000
    coordinator_container_name = DB_VOL_NAME_BASE+"coordinator"
    coordinator_vol_name = CONTAINER_NAME_BASE+"coordinator_vol"
    call_cmd(["docker","run","--name",coordinator_container_name,\
                        "--rm",\
                        "--volume="+coordinator_vol_name+":/var/lib/postgresql/data",\
                        "-p",str(COORDINATOR_DB_PORT)+":5432",\
                        "--shm-size=4G",\
                        "-e","POSTGRES_PASSWORD=password",\
                        "-d","postgres:12.1",\
                        "-N","10"])

    for agent_id in range(num_agents):
        agent_vol_name = DB_VOL_NAME_BASE+"agent_val_"+str(agent_id)
        agent_container_name = CONTAINER_NAME_BASE+"agent_"+str(agent_id)
        agent_db_port = AGENT_DB_PORTS_STARTS_AT + agent_id
        call_cmd(["docker","run","--name",agent_container_name,\
                        "--rm",\
                        "--volume="+agent_vol_name+":/var/lib/postgresql/data",\
                        "-p",str(agent_db_port)+":5432",\
                        "--shm-size=4G",\
                        "-e","POSTGRES_PASSWORD=password",\
                        "-d","postgres:12.1",\
                        "-N","10",\
                        "-c","max_prepared_transactions=32"])


def init_dbs(num_agents):
    os.chdir(PROJECT_DIR_PATH)
    #Init coordinator db (for logging)
    print("Initiate DB (for log) for coordinator ")
    call_cmd(["./init_coordinator_postgres_log.sh",str(COORDINATOR_DB_PORT)])

    for agent_id in range(num_agents):
        #init DBs (data & log) for agents
        print("Initiate DB (data) for agent "+ str(agent_id))
        agent_db_port = AGENT_DB_PORTS_STARTS_AT + agent_id 
        call_cmd(["./init_cohort_postgres_low_concurrency.sh",str(agent_db_port)])
        print("Initiate Log DB (log) for agent "+ str(agent_id))
        call_cmd(["./init_cohort_postgres_log.sh",str(agent_db_port)])

def runexp(mpl, num_agents, simulated_error, error_transaction_id, skip_db_init, num_of_tx_in_total):
    if(skip_db_init is False):
        create_db_vols(num_agents)
        start_db_containers(num_agents)
        time.sleep(3)
        init_dbs(num_agents)

    os.chdir(PROJECT_DIR_PATH + "/experiment")
    subprocess.call(["gradle","clean"])
    subprocess.call(["gradle","buildCoordinator"])
    subprocess.call(["gradle","buildAgent"])

    now = datetime.now()
    experiment_id = now.strftime("%Y-%m-%d-%H-%M-%S")

    output_dir = PROJECT_DIR_PATH + "/results/" + "errID_" + str(simulated_error) + "|transID_" + str(error_transaction_id) + "|expID_" + experiment_id
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_dir + "/transactions").mkdir(parents=True, exist_ok=True)

    for agent_id in range(num_agents):
        agent_output_file_path = output_dir + "/agent_" + str(agent_id) + "_output.txt"
        agent_output_file = open(agent_output_file_path, "w")
        agent_db_port = AGENT_DB_PORTS_STARTS_AT + agent_id
        agent_app_port = AGENT_APP_PORTS_STARTS_AT + agent_id
        agent_proc = call_cmd_no_blocking(["java", "-jar" ,"-Xmx32g", "build/libs/experiment-agent-all-0.1.jar", \
                            "-a", str(agent_id),\
                            "-m", str(mpl),\
                            "-d", str(agent_db_port),\
                            "-p", str(agent_app_port),\
                            "-i", experiment_id,\
                            "-e", str(simulated_error),\
                            "-t", str(error_transaction_id)], agent_output_file)
        proc_list.append(agent_proc)

    time.sleep(2)

    coordinator_output_file_path = output_dir + "/coordinator"  + "_output.txt"
    coordinator_output_file = open(coordinator_output_file_path, "w")
    coordinator_proc = call_cmd_no_blocking(["java", "-jar" ,"-Xmx32g", "build/libs/experiment-coordinator-all-0.1.jar",\
                            "-m", str(mpl),\
                            "-w", "low", \
                            "-p", "simplebatch", \
                            "-c", str(num_of_tx_in_total),\
                            "-d", str(COORDINATOR_DB_PORT), \
                            "-n", str(num_agents),\
                            "-s", str(AGENT_APP_PORTS_STARTS_AT),\
                            "-i", experiment_id,\
                            "-e", str(simulated_error),\
                            "-t", str(error_transaction_id)], coordinator_output_file)
    print("Coordinator Process Starts, It will load all insertion statements into memory first, it will take some time to finish loading first (roughly 1-2 minutes for low concurrency dataset on a machine with SSD) ")
    print("Please find the 'result' directory for the stdout output for coordinator and agent. 'result/transaction' has the output log for the transaction 2PC execution on both coordinator and agent")
    proc_list.append(coordinator_proc)

    while(True):
        pass



print("Please make sure the clean_exp.py is executed to remove the previous DB containers and volumes")

runexp(mpl=2,num_agents=3,simulated_error=0,error_transaction_id=10,skip_db_init=False,num_of_tx_in_total=1000)