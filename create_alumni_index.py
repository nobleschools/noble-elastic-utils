"""
create_alumni_index.py

Creates a shared Elasticsearch index to hold alumni data, starting
from the FIRST_DATA_YEAR.

Creates fake indices for each campus using aliases.
cf. https://www.elastic.co/guide/en/elasticsearch/guide/current/faking-it.html
"""

from datetime import datetime
from os import pardir, path
import sys
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from elasticsearch.helpers import bulk as es_bulk_action
from elasticsearch_dsl import DocType, Index, Integer, Text
from elasticsearch_dsl.connections import connections as es_connections
from simple_salesforce import Salesforce
import requests

from constants import CAMPUS_SF_IDS, FIRST_DATA_YEAR
from secrets import salesforce_secrets as sf_secrets
from secrets.elastic_secrets import ES_CONNECTION_KEY as ES_KEY


def bulk_alum_gen(campus, action='create'):
    """
    Yields dictionaries of info from Salesforce for all alumni from the
    campus given, starting from the cutoff year (FIRST_DATA_YEAR),
    in a format ready for `elasticsearch.helpers.bulk` action.

    The dictionaries include metadata for Elasticsearch:
        `_op_type`: action to take on document, eg. 'create', 'update', etc
        `_index`  : index ('alumni') to which the document belongs
        `_type`   : document type ('alum')
        `_id`     : document's id/pk; set to the alum's Noble network ID
    And finally the document itself:
        `_source` : alum-specific data

    Arguments:

    * campus: name of the campus from which to generate alumni; see the
              `CAMPUS_SF_IDS` lookup for valid parameters.

    Available kwargs:

    * action: action to take in elasticsearch (create, update, delete, index)
    """
    # share connection?
    sf_connection = Salesforce(
        username=sf_secrets.SF_LIVE_USERNAME,
        password=sf_secrets.SF_PASSWORD,
        security_token=sf_secrets.SF_LIVE_TOKEN,
    )
    alumni_query = ("SELECT Safe_Id__c, Network_Student_ID__c, LastName, "
                    "FirstName, Name, HS_Class__c, Facebook_ID__c, OwnerId "
                    "FROM Contact "
                    "WHERE AccountID = '{}' AND HS_Class__c >= '{}'")

    campus_sf_id = CAMPUS_SF_IDS[campus] # or just fail
    results = sf_connection.query(
        alumni_query.format(campus_sf_id, FIRST_DATA_YEAR)
    )

    counts = 0 # spit out at end?
    while True:
        for record in results['records']:
            counts += 1
            # use empty string  where no Facebook ID on file in Salesforce;
            # uniqueness not enforced in Elasticsearch
            facebook_id = ''
            if record['Facebook_ID__c']:
                facebook_id = record['Facebook_ID__c']
            source = {
                'safe_id': record['Safe_Id__c'],
                'campus': campus,
                'last_name': record['LastName'],
                'first_name': record['FirstName'],
                'full_name': record['Name'],
                'class_year': int(record['HS_Class__c']),
                'facebook_id': facebook_id,
                'ac_safe_id': record['OwnerId'],
            }
            new_document = {
                '_op_type': action,
                '_index': 'alumni',
                '_type': 'alum',
                '_id': int(record['Network_Student_ID__c']),
                '_source': source,
            }

            yield new_document

        if not results['done']:
            results = sf.query_more(results['nextRecordsUrl'], True)
        else:
            print("{:>15}: {}".format(campus, counts))
            break


def ensure_alumni_index():
    """
    Make sure it exists, along with the mapping for the alum objects, and
    create aliases to campuses so that they are accessed as if they were
    separate indices.
    """
    alumni_index = Index('alumni')

    # build mapping for object type
    # can also be used as class decorator when defining the DocType
    @alumni_index.doc_type
    class Alum(DocType):
        campus = Text() # also used to create aliases as fake indices
        last_name = Text()
        first_name = Text()
        full_name = Text()
        #noble_id = Integer() # will be object ID in ES
        safe_id = Text()
        class_year = Integer()
        facebook_id = Text()
        ac_safe_id = Text() # OwnerId field on Contact obj in Salesforce

    # save the index
    # bug in error handling (due to version mismatch?)
    # https://github.com/elastic/elasticsearch-py/issues/496
    try:
        alumni_index.create(ignore=400)
    except AuthenticationException as e:
        print(e.error)
        raise e

    create_campus_aliases()


def create_campus_aliases():
    """
    Create an alias in the 'Alumni' index for each campus, allowing a
    pseudo-indexing API; eg. /alumni/alum/<campus> etc.. -> /<campus>/alum/..
    """
    for campus in CAMPUS_SF_IDS.keys():
        add_es_alias(campus)


def add_es_alias(campus_name):
    """
    Adds an alias to ES for the campus_name, so that Alum documents with
    campus=campus_name can be accessed via psuedo-index API, eg. /<campus>/alum/...
    """
    put_data = {
        "routing": campus_name,
        "filter": {
            "term": {
                "campus": campus_name,
            }
        }
    }
    dest = '{}/alumni/_alias/{}'.format(ES_KEY, campus_name)
    r = requests.put(dest, json=put_data)
    r.raise_for_status()

    return r.ok


def create_alumni_index():
    """
    Create the index in Elasticsearch, aliases the campuses as fake indices,
    and populates it with alumni data starting from the FIRST_DATA_YEAR.
    """
    es_connection = es_connections.create_connection(hosts=[ES_KEY], timeout=20)
    ensure_alumni_index()
    for campus_name in CAMPUS_SF_IDS.keys():
        bulk_gen = bulk_alum_gen(campus_name, action='create')
        es_bulk_action(es_connection, bulk_gen)


if __name__ == "__main__":
    create_alumni_index()
