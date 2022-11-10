import requests
from requests import HTTPError
import asyncio
import aiohttp
from asyncio_throttle import Throttler
from functools import partial
from datetime import datetime as dt
from pathlib import Path
import os
import json
import logging
from config import config
import click

# Throttles async connections
throttler = Throttler(rate_limit=10)

# Set logging level
logging.basicConfig(level=config.get('log_level', 'INFO'))

def authenticate(config: dict):
    '''
    Given config object, authenticate to Aspace, returning a token (str).
    '''
    try:
        r = requests.post(config['base_url'] + f'users/{config["user"]}/login', 
                          params={'password': config['password']})
        r.raise_for_status()
        return r.json().get('session')
    except HTTPError:
        # Propagate execption here, because we can't continue if unauthenticated
        logging.error(f'Authentication failed for user {config["user"]} with password {config["password"]} with error {r.text}.')
        raise

def create_auth_header(token: str):
    '''
    Creates an ASpace authentication header, given a token. Returns the header (dict).
    '''
    return {'X-ArchivesSpace-Session': token}


def get_all_ids(header: dict, obj_type: str='resources', modified_since: int=None):
    '''
    Requests all object IDs for objects of the specified type from the repository. 
    :param header: dict for authenticating the request
    :param obj_type: a str specifiying one of resources, digital_objects, accessions
    :param modified_since: an int representing a UNIX timestamp: mapped to the system_mtime field in the ASpace records
    Yields object IDs (-> int)
    '''
    params = {'all_ids': True}
    if modified_since:
        params['modified_since'] = modified_since
    r = requests.get(f'{config["base_url"]}/repositories/{config["repo"]}/{obj_type}',
                     params=params,
                     headers=header)
    try:
        r.raise_for_status()
        for id_ in r.json():
            yield id_
    except HTTPError:
        logging.error(f'Error requesting objects of type {obj_type}: {r.text}')


def make_urls(obj_ids: list, obj_type: str='resources'):
    '''
    Constructs the URLs for the given ASpace object IDs
    :param obj_ids: a list of ints or strings
    :param obj_type: one of resources, digital_objects, accessions
    Returns a list of strs, and None (for params)
    '''
    return [f'{config["base_url"]}/repositories/{config["repo"]}/{obj_type}/{obj_id}'
            for obj_id in obj_ids], None


def make_ead_urls(resource_ids: list):
    '''
    Constructs the URLs for the resource descriptions for the given ASpace resource IDs (list of str/int). Returns a list of str and a dict of (str, str)
    '''
    params = {'include_unpublished': config['include_unpublished'],
              'include_daos': config['include_daos'],
              'numbered_cs': config['numbered_cs']}
    urls = [f'{config["base_url"]}/repositories/{config["repo"]}/resource_descriptions/{resource_id}.xml' for resource_id in resource_ids]
    return urls, params

def make_mets_urls(do_ids: list):
    '''
    Constructs the URL's for digital-objects METS documents for the provided digital object ID's (list of str/int)
    Returns list of str and None for params.
    '''
    urls = [f'{config["base_url"]}/repositories/{config["repo"]}/digital_objects/mets/{do_id}.xml'
           for do_id in do_ids]
    return urls, None

async def retrieve_obj(obj_url: str, session, throttler, params=None):
    '''
    Given :param obj_url: an ASpace URL to an object (resource, resource description, etc.), retrieve the record associated with it.
    Async function for running with asyncio.run
    :param client: an aiohttp client session
    :param throttler: instance of asyncio_throttle.Throttler, for setting rate limits
    :param params: a dict of URL params (optional)
    Returns a dict either of the ASPace object or containing an XML str
    '''
    async with throttler:
        async with session.get(obj_url, params=params) as resp:
            try:
                assert resp.status == 200
                if resp.headers['Content-Type'] == 'application/json':
                    return await resp.json()
                else:
                    # Return XML with URI for clarity
                    return await {'uri': obj_url,
                                'body': resp.text()}
            except AssertionError:
                error = await resp.text()
                logging.error(f'Error with request for URL {obj_url}: {error}')
                # Return the error so that we can keep track
                return {'uri': obj_url,
                        'error': error}

# Throttled version of the above
retrieve_obj_rl = partial(retrieve_obj, throttler=throttler)

async def retrieve_objs(ids: list, url_func, header: dict):
    '''
    Calls the API asynchronously on a given list of ids.
    :param ids: list of identifiers
    :param url_func: a function to create the API URL and params from the given identifiers
    :param header: the ASpace authentication header
    Returns a list of dicts (ASpace objects, XML docs, and/or errors)
    '''
    urls, params = url_func(ids)
    async with aiohttp.ClientSession(headers=header) as session:
        resource_tasks = [retrieve_obj_rl(url, session, params=params) for url in urls]
        return await asyncio.gather(*resource_tasks)

def retrieve_xml(record_ids: list, url_func, header: dict):
    '''
    Retrieves the XML representations for a given list of record ID's. Makes calls in sequence to avoid overloading the ASpace server.
    :param url_func: function to return the URL's for the XML docs from the list of ID's. 
    :param header: the ASpace authentication header
    Returns a list of dicts containing either XML strs or error messages
    '''
    docs = []
    urls, params = url_func(record_ids)
    for url in urls:
        r = requests.get(url, params=params, headers=header)
        try:
            r.raise_for_status()
            docs.append({'uri': url,
                        'body': r.text})
        except HTTPError:
            logging.error(f'Error for URL {url}: {r.text}')
            docs.append({'uri': url,
                        'error': r.text})
    return docs

# Function to construct full URL's from a list of URI references (e.g., for retrieving containers associated with a given record)
make_container_urls = lambda refs: ([config['base_url'] + ref['ref'] for ref in refs], None)

def get_container_refs(resource_ids: list, header: dict):
    '''
    For a list (int/str) of resource ID's, yields, for each associated container, a dict containing the resource ID (str/int) and the container reference (str).
    :param header: ASpace authentication header.
    '''
    # Make a copy so that we don't modify the original list
    resource_ids = resource_ids[:]
    while resource_ids:
        r_id = resource_ids.pop()
        r = requests.get(f'{config["base_url"]}/repositories/{config["repo"]}/resources/{r_id}/top_containers',
                headers=header)
        try:
            r.raise_for_status()
            for ref in r.json():
                ref.update({'resource_id': r_id})
                yield ref
        except HTTPError:
            # If there's a gateway timeout, try again for this resource
            if r.status_code == 504:
                resource_ids.append(r_id)
                logging.error(f'Gateway timeout on {r_id}. Will try again later.')
            logging.error(f'Error requesting container refs for resource {r_id}: {r.text}')


def store_objects(objects: list, paths: list=None):
    '''
    Stores ASpace objects, using the base_local_path configuration parameter; 
    plus a) for JSON objects, the path indicated in the uri field, or b) for XML docs, a provided list of paths.
    :param objects: list of dict or list of str
    :param paths: list of str: if provided, should match the list of objects.
    '''
    # Case 1: path embedded in object uri
    if not paths:
        for obj in objects:
            # Need to do this to relativize the URI 
            path = config['base_local_path'] / Path(os.path.relpath(Path(obj['uri']), start='/'))
            # Create the path if it doesn't already exist
            path.parent.mkdir(parents=True, exist_ok=True)
            # Assume JSON if we've gotten this far
            with path.with_suffix('.json').open(mode='w') as f:
                json.dump(obj, f)
    # Case 2: object is txt/xml; paths supplied
    else:
        for obj, path in zip(objects, paths):
            path = config['base_local_path'] / Path(os.path.relpath(path, start='/'))
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.with_suffix('.xml').open(mode='w') as f:
                f.write(obj)

def get_resources_runner(resource_ids: list, header: dict):
    '''
    Helper function for running async task with Airflow.
    :param resource_ids: list of ASpace resource IDs
    :param header: ASpace authentication header
    '''
    return asyncio.run(retrieve_objs(resource_ids, partial(make_urls, obj_type='resources'), header))

def get_digital_objects_runner(do_ids: list, header: dict):
    '''
    Helper function for running async task with Airflow.
    :param do_ids: list of ASpace digital object IDs
    :param header: ASpace authentication header
    '''
    return asyncio.run(retrieve_objs(do_ids, partial(make_urls, obj_type='digital_objects'), header))

def get_resource_ids(header: dict):
    '''
    Helper function for calling get_all_ids
    :param header: ASpace authentication header
    '''
    return [r for r in get_all_ids(header)]

def get_do_ids(header: dict):
    '''
    Helper function for calling get_all_ids
    :param header: ASpace authentication header
    '''
    return [r for r in get_all_ids(header, obj_type='digital_objects')]

@click.command()
@click.option('--test', help='Set an integer to run a test on a subset of objects.')
def main(test: int=0):
    '''
    Main entrypoint for calling from command line.
    :param test: Assign an integer greater than 0 to test on a subset of documents
    '''
    # Get auth header
    header = create_auth_header(authenticate(config))
    # Get ID's for resources
    resource_ids = get_resource_ids(header)
    # Get digital object IDs
    do_ids = get_do_ids(header)
    if test:
        resource_ids = resource_ids[:test]
        do_ids = do_ids[:test]
    # Get resources
    resources = get_resources_runner(resource_ids, header)
    # Get EAD finding aids
    ead = retrieve_xml(resource_ids, make_ead_urls, header)
    # Get digital objects
    dos = get_digital_objects_runner(do_ids, header)
    # Get METS documents
    mets = retrieve_xml(do_ids, make_mets_urls, header)
    # Save objects, excluding errors
    for objs in [resources, dos]:
        store_objects(objs.filter(lambda x: 'error' not in x))
    # Save XML docs
    for docs in [ead, mets]:
        # Exclude errors
        docs_to_store = [d for d in docs if 'error' not in d]
        # Generate paths: removing the initial part of the URI 
        paths = [d['uri'].replace(config['base_url'], '') for d in docs_to_store]
        # XML is under the "body" key
        store_objects(docs_to_store.map(lambda x: x['body']), paths)

if __name__ == '__main__':
    main()

