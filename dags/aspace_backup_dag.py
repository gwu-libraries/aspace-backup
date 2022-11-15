import pendulum
import sys
from airflow.decorators import dag, task

sys.path.append('scripts/aspace_backup')

from scripts.aspace_backup.backup_aspace import main

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 11, 15),
    catchup=False,
    tags=['test'],
)
def test_aspace_backup():
    '''
    '''
    @task
    def run():
        main(test=10)
test_aspace_backup()