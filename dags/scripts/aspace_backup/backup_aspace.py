import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry
import asyncio
import aiohttp
from asyncio_throttle import Throttler
from functools import partial
from datetime import datetime as dt
from pathlib import Path
import os
import json
import logging
import click

# Set logging level
#logger = logging.getLogger(__name__)
#logger.addHandler(logging.StreamHandler().setLevel(config.get('log_level', 'INFO')))

def authenticate(config: dict):
    '''
    Given config object, authenticate to Aspace, returning a token (str).
    '''
    try:
        r = requests.post(config['base_url'] + f'/users/{config["user"]}/login', 
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


class BaseObjFetch:
    '''
    Base class with functionality for working with the ASpace API.
    '''
    def __init__(self, throttler):
        '''
        :param throttler: an async throttler for rate-limiting the API
        '''
        # Throttled version of the retrieve_obj function
        self.retrieve_obj = partial(BaseObjFetch.retrieve_obj, throttler=throttler)
      
    def get_all_ids(self, modified_since: int=None):
        '''
        Requests all object IDs for objects of the specified type from the repository. 
        :param modified_since: an int representing a UNIX timestamp: mapped to the system_mtime field in the ASpace records
        Yields object IDs (-> int)
        '''
        params = {'all_ids': True}
        if modified_since:
            params['modified_since'] = modified_since
        r = requests.get(f'{self.config["base_url"]}/repositories/{self.config["repo"]}/{self.obj_type}',
                        params=params,
                        headers=self.header)
        try:
            r.raise_for_status()
            for id_ in r.json():
                yield id_
        except HTTPError:
            logging.error(f'Error requesting objects of type {self.obj_type}: {r.text}')
    
    def make_urls(self):
        '''
        Constructs the URLs for the given ASpace object IDs
        Returns a list of strs, and None (for params)
        '''
        return [f'{self.config["base_url"]}/repositories/{self.config["repo"]}/{self.obj_type}/{id_}'
                for id_ in self.ids], None

    @staticmethod
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

    async def retrieve_objs(self, url_func):
        '''
        Calls the API asynchronously on a given list of ids.
        :param url_func: a function to create the API URL and params from the given identifiers
        Returns a list of dicts (ASpace objects, XML docs, and/or errors)
        '''
        urls, params = url_func()
        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.retrieve_obj(url, session, params=params) for url in urls]
            return await asyncio.gather(*tasks)
    

    def retrieve_xml(self, url_func):
        '''
        Retrieves the XML representations for a given list of record ID's. Makes calls in sequence to avoid overloading the ASpace server.
        :param url_func: function to return the URL's for the XML docs from the list of ID's. 
        Returns a list of dicts containing either XML strs or error messages
        '''
        # Setting retries to avoid ConnectionError with ASpace API
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1)
        session.mount('https://', HTTPAdapter(max_retries=retries))
        docs = []
        urls, params = url_func()
        for url in urls:
            r = session.get(url, params=params, headers=self.header)
            try:
                r.raise_for_status()
                docs.append({'uri': url,
                            'body': r.text})
            except HTTPError:
                logging.error(f'Error for URL {url}: {r.text}')
                docs.append({'uri': url,
                            'error': r.text})
        return docs

    def store_objects(self, objects: list, paths: list=None):
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
                path = self.config['base_local_path'] / Path(os.path.relpath(Path(obj['uri']), start='/'))
                # Create the path if it doesn't already exist
                path.parent.mkdir(parents=True, exist_ok=True)
                # Assume JSON if we've gotten this far
                with path.with_suffix('.json').open(mode='w') as f:
                    json.dump(obj, f)
        # Case 2: object is txt/xml; paths supplied
        else:
            for obj, path in zip(objects, paths):
                path = self.config['base_local_path'] / Path(os.path.relpath(path, start='/'))
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.with_suffix('.xml').open(mode='w') as f:
                    f.write(obj)


class ObjectFetch(BaseObjFetch):
    '''
    Subclass for getting ASpace object records (resources, digital objects, etc.) and XML docs.
    '''
    def __init__(self, config: dict, header: dict, obj_type: str='resources'):
        '''
        :param config: dict of ASpace URL components
        :param header: ASpace authentication header
        :param obj_type: the type of ASpace object this class retrieves
        :param throttler: an async throttler for rate-limiting the API
        '''
        self.config = config
        self.header = header
        self.obj_type = obj_type
        # Throttles async connections
        self.throttler = Throttler(rate_limit=10)
        # Initialize base class with throttler
        super().__init__(self.throttler)
    
    def make_ead_urls(self):
        '''
        Constructs the URLs for the resource descriptions for the given ASpace resource IDs (list of str/int). 
        '''
        params = {'include_unpublished': self.config['include_unpublished'],
                'include_daos': self.config['include_daos'],
                'numbered_cs': self.config['numbered_cs']}
        urls = [f'{self.config["base_url"]}/repositories/{self.config["repo"]}/resource_descriptions/{id_}.xml' for id_ in self.ids]
        return urls, params
    
        
    def make_mets_urls(self):
        '''
        Constructs the URL's for digital-objects METS documents for the digital object ID's (list of str/int)
        Returns list of str and None for params.
        '''
        urls = [f'{self.config["base_url"]}/repositories/{self.config["repo"]}/digital_objects/mets/{id_}.xml'
            for id_ in self.ids]
        return urls, None
    
    def get_object_ids(self, modified_since: int=None, test: int=0):
        '''
        Helper function for calling get_all_ids
        :param modified_since: an int representing a UNIX timestamp: mapped to the system_mtime field in the ASpace records
        :param test: optional number to limit number of results, for testing
        '''
        self.ids = [r for r in self.get_all_ids(modified_since=modified_since)]
        if test:
            self.ids = self.ids[:test]
        return self

    def get_objects(self):
        '''
        Helper function for running async task with Airflow.
        '''
        self.objects = asyncio.run(self.retrieve_objs(self.make_urls))
        return self

    def get_xml_docs(self, url_func):
        '''
        Retrieve EAD docs for each resource
        :param url_func: function to create the URL's for the XML docs from self.ids
        '''
        self.docs = self.retrieve_xml(url_func)
        return self
     
    def make_container_urls(self, refs: list):
        '''
        Constructs full URL's from a list of URI references (e.g., for retrieving containers associated with a given record)
        :param refs: list of dict (containing a ref key)
        '''
        return [self.config['base_url'] + ref['ref'] for ref in refs], None
        
    def get_container_refs(self):
        '''
        For a list (int/str) of resource ID's, yields, for each associated container, a dict containing the resource ID (str/int) and the container reference (str).
        '''
        # Make a copy so that we don't modify the original list
        resource_ids = self.ids[:]
        while resource_ids:
            r_id = resource_ids.pop()
            r = requests.get(f'{self.config["base_url"]}/repositories/{self.config["repo"]}/resources/{r_id}/top_containers', headers=self. header)
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

    def get_deletes(self):
        '''
        Retrieves URIs from the delete-feed endpoint of ASpace in order to identify objects that have been deleted.
        Returns a list of str of refs to deleted objects (URI's)
        '''
        deletes = []
        last_page = None
        # Pagination starts with 1
        this_page = 1
        while not last_page or this_page <= last_page:
            response = requests.get(self.config['base_url'] + '/delete-feed', 
                                    params={'page': this_page}, 
                                    headers=self.header)
            try:
                response.raise_for_status()
                results = response.json()
                deletes.extend(results['results'])
                last_page = results['last_page']
                this_page += 1
            except HTTPError:
                logging.error(f'Error accessing delete-feed on page {this_page} of results.')
                raise
        # Return only those deleted object URI's of the specified type
        return [r for r in deletes if self.obj_type in r]


@click.command()
@click.option('--test', type=int, help='Set an integer to run a test on a subset of objects.')
@click.option('--object_type', type=str, help='Run only for the specified type of ASpace object: either resources or digital_objects')
def main(test: int=0, object_type=None):
    '''
    Main entrypoint for calling from command line.
    :param test: Assign an integer greater than 0 to test on a subset of documents
    '''
    # Import config file here for main entrypoint
    # If we import globally, it won't work with Airflow
    from config import config
    logging.basicConfig(level=config.get('log_level', 'INFO'))
    # Get auth header
    header = create_auth_header(authenticate(config))
    # for resource records
    if (not object_type) or (object_type == 'resources'):
        rf = ObjectFetch(config=config, header=header)
        logging.info('Getting resource IDs and objects')
        rf.get_object_ids(test).get_objects()
        logging.info('Getting EAD docs')
        rf.get_xml_docs(rf.make_ead_urls)
        logging.info('Saving objects and documents')
        rf.store_objects([obj for obj in rf.objects if 'error' not in obj])
        docs_to_store = [d for d in rf.docs if 'error' not in d]
        # Generate paths: removing the initial part of the URI 
        paths = [d['uri'].replace(config['base_url'], '') for d in docs_to_store]
        # XML is under the "body" key
        rf.store_objects([doc['body'] for doc in docs_to_store], paths)
    # for digital objects
    if (not object_type) or (object_type == 'digital_objects'):
        do = ObjectFetch(config=config, header=header, obj_type='digital_objects')
        logging.info('Getting digital object IDs and objects')
        do.get_object_ids(test).get_objects()
        logging.info('Getting METS docs')
        do.get_xml_docs(do.make_mets_urls)
        logging.info('Saving objects and documents')
        do.store_objects([obj for obj in do.objects if 'error' not in obj])
        docs_to_store = [d for d in do.docs if 'error' not in d]
        # Generate paths: removing the initial part of the URI 
        paths = [d['uri'].replace(config['base_url'], '') for d in docs_to_store]
        # XML is under the "body" key
        do.store_objects([doc['body'] for doc in docs_to_store], paths)

if __name__ == '__main__':
    main()

