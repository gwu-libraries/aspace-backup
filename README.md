# ASpace Backup
Script to backup ASpace objects using the API.

The script can be run either by itself on the command line or via Apache Airflow. 

## Running from the command line
1. Clone this repository.
2. In the `dags/scripts/aspace_backup` directory, copy `example.config.py` to `config.py`. This file should contain the ASpace API username and password, the base URL for the repository and the repository ID, as well as the path to a local directory for storing the data.
3. It's recommended to create a Python virtual environment: `python -m venv ENV` before install dependencies.
4. After activating the virtual environment, install the requirements (from the root directory of the repository): `pip install -r requirements.txt`.
5. Run the script: `python dags/scripts/aspace_backup/backup_aspace.py`
   - By default, it will perform a complete backup of both ASpace resources and digital objects, along with the EAD finding aids and the METS XML versions of the digital objects.
   - To specify one or the other type of object for backup use the `--object_type` command-line option, followed by one of `resources`, `digital_objects`.
   - To perform a test on a smaller set of records, use the `--test` option with a number N. The first N records will be retrieved.
   - JSON objects and XML documents are saved to the path specified in `config.py`, perserving the folder structure indicated in the object URI.
   
## Running with Apache Airflow

This implementation uses Airflow 2.4.3 in Dockerized form with Python 3.8. 

1. Follow steps 1 & 2 of the above. 
2. On Linux/Ubuntu, it's recommended to run Airflow in Docker as your user (rather than root). To do so, create a `.env` file in the root directory of the repo and add the following line:
   ```
   AIRFLOW_UID=XXXXXXX
   ```
   where `XXXXXXX` is the result of running `id -u` at the command line.
3. Create new directories in the repo called `logs` and `plugins`. (These are mapped into the Airflow containers.)
4. Run `docker compose up airflow-init` to initialize the Airflow database.
5. The DAG file is `dags/aspace_backup_dag.py`. In this file, you can set the `start_date` and `schedule` parameters as necessary. The `start_date` should be the date of the initial run of the DAG. The `schedule` param takes a cron-formatted string. 
6. Run `docker compose up -d` to start the containers.
6. The initial run of the DAG will perform a full backup. Subsequent backups will retrieve only those ASpace objects modified since the timestamp of the last successful DAG run.
7. Manual backups can be triggered from the Airflow UI.
8. To view the UI (from a remote host), use port forwarding from your local machine to listen on port 8080: e.g., ` ssh -L 8080:localhost:8080 username@remotehost`-- where `username` is your username on the remote host, and `remotehost` is the host running Airflow.

For more information on running Airflow in a Docker environment, see [the Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
