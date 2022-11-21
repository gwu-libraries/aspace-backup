import pendulum
import sys
from airflow.decorators import dag, task
from scripts.aspace_backup import backup_aspace as bua
from scripts.aspace_backup.config import config
import logging


@task
def get_objects(obj_type: str='resources', test: int=0, modified_since=None, **kwargs):
    '''
    Reusable task to fetch objects of the specified type from the ASpace API.
    :param obj_type: should be one of ['resources', 'digital_objects']
    '''
    logger = logging.getLogger("airflow.task")
    # If DAG has run before, assume full backup has been done; get only resources modified since the last successful run
    if kwargs['prev_start_date_success']:
        modified_since = int(kwargs['prev_start_date_success'].timestamp())
        logger.info(f'Getting resources modified since {kwargs["prev_start_date_success"]}')
    # Create ASpace header with token
    header = bua.create_auth_header(bua.authenticate(config))
    # Get resource objects and IDs
    obj_fetch = bua.ObjectFetch(config=config, header=header, obj_type=obj_type)
    obj_fetch.get_object_ids(modified_since=modified_since, test=test).get_objects()
    logger.info(f'Got {len(obj_fetch.objects)} {obj_type}.')
    # Get XML EAD docs
    if obj_type == 'resources':
        obj_fetch.get_xml_docs(obj_fetch.make_ead_urls)
    else:
        obj_fetch.get_xml_docs(obj_fetch.make_mets_urls)
    logger.info(f'Got {len(obj_fetch.docs)} XML docs for {obj_type}.')
    # Store objects
    obj_fetch.store_objects([obj for obj in obj_fetch.objects if 'error' not in obj])
    # Store XML docs
    docs_to_store = [d for d in obj_fetch.docs if 'error' not in d]
    paths = [d['uri'].replace(config['base_url'], '') for d in docs_to_store]
    obj_fetch.store_objects([doc['body'] for doc in docs_to_store], paths)
    # Log errors
    errors = [obj for obj in obj_fetch.objects if 'error' in obj] + [d for d in obj_fetch.docs if 'error' in d]
    for error in errors:
        logger.error(error)

@dag(
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2022, 11, 17, tz='EST'),
    catchup=False,
    tags=['prod', 'aspace', 'backup'],
)
def aspace_backup():
    '''
    Get extract of ASpace resources and digital objects. If running for the first time, it will perform a full extract. Otherwise, it will get only those records modified since the last run.
    '''
    resource_task = get_objects.override(task_id='get_resources')
    do_task = get_objects.override(task_id='get_digital_objects')
    resource_task() >> do_task(obj_type='digital_objects')

aspace_backup()
