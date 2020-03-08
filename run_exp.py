import os
import subprocess

def runexp(workload, db, policy, mpl, isolation):
    os.chdir("/home/guoxi/Workspace/cs223")
    subprocess.call(["./init_"+db+"_"+workload+"_concurrency.sh"])
    os.chdir("/home/guoxi/Workspace/cs223/experiment")
    subprocess.call(["gradle","clean"])
    subprocess.call(["gradle","fatJar"])
    subprocess.call(["java", "-jar" ,"-Xmx32g", "build/libs/experiment-all-0.1.jar", "-w", workload, "-d", db, "-p", policy, "-m", mpl, "-i", isolation])


# runexp("low","postgres", "batch", "4", "2")

def run_exp1():
    for workload in ["low","high"]:
        for db in ["postgres", "mysql"]:
            for policy in ["single","batch"]:
                for mpl in ["2", "4", "8", "16", "32"]:
                    for count in [1,2]:
                        runexp(workload, db, policy, mpl, "2")

def run_exp1_more():
    for workload in ["low","high"]:
        for db in ["postgres", "mysql"]:
            for policy in ["single","batch"]:
                for mpl in ["64","128"]:
                        runexp(workload, db, policy, mpl, "2")

def run_exp2():
    for workload in ["low","high"]:
        for db in ["postgres", "mysql"]:
            for isolation in ["1","4","8"]:
                for mpl in ["4", "8", "16", "32","64","128"]:
                    runexp(workload, db, "batch", mpl, isolation)

def run_exp_ext256512():
    for workload in ["low","high"]:
        for db in ["postgres", "mysql"]:
            for policy in ["single","batch"]:
                for isolation in ["1","2","4","8"]:
                    for mpl in ["256","512"]:
                        runexp(workload, db, policy, mpl, isolation)


#run_exp1()
# run_exp1_more()
# run_exp2()
run_exp_ext256512()