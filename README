## Software Requirements

**Supported OS**: Linux / macOS

**Required Softwares**:

- Bash
- Gradle: (version: 6.0+)
- Java (JDK): (version: 11+)
- Docker (version: 19+)
- Python 3.6.9



## Brief Intro

The whole experiment program is designed to be portable. The DB softwares run in docker containers and exposes the ports to the host machine for the experiment program (both coordinator and agents) to connect with. The experiment program is written in Java and the dependency packages and compilation/build process are managed by Gradle. Python scripts are run the experiments and clean up the experiments.



## Run the experiment

Please follow the following steps to set up the environment and run the experiments:

1. Create two folders in the project directory (if not exist):

   - `./results/`

   - `./inputs/`

2. Setup correct path variables according to the path where you put the project directory root:
   - in `./run_exp.py`: 
     - Modify `PROJECT_DIR_PATH` variable: the absolute path to the project root folder
   - in `./experiment/src/main/resources/experiment.properties`:
     - Modify `result.output_path` property: the absolute path to the `results` folder
     - Modify `replayer.inputs_directory `property: the absolute path to the `input` folder

3. Copy the data files (data&schemaqueries) into the inputs folder

4. Modify the experiment parameters in `./run_exp.py` at the last line (arguments in `runexp()` function)

   (`simulated_error` and `error_transaction_id` are not used now. They are previously used for testing and no longer useful)

5. Run the experiment by executing: `./run_exp.py`

6. Before running experiment next time, you need to execute: `./clean_exp.py [n]` , [n] is the number of agents you specified in the last experiment (in `run_exp.py`). This script will clean up the docker volumes (for persistent DB storage) and docker containers.

