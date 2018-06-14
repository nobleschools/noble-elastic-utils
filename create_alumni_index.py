"""
create_alumni_index.py

Creates a shared Elasticsearch index to hold alumni data.

Creates fake indices for each campus using aliases, to skirt around the index
limit on our current tier on Bonsai.
cf. https://www.elastic.co/guide/en/elasticsearch/guide/current/faking-it.html
"""

from elasticsearch.helpers import bulk as es_bulk_action
from elasticsearch_dsl import DocType, Index, Integer, Text
from elasticsearch_dsl.connections import connections as es_connections
import requests

from salesforce_utils.constants import CAMPUS_SF_IDS
from salesforce_utils.get_connection import get_salesforce_connection
from secrets.elastic_secrets import ES_CONNECTION_KEY as ES_KEY

ALUMNI_INDEX = "alumni"
ALUMNI_DOC_TYPE = "alum"

def _bulk_alum_gen(campus, action="create"):
    """Yields dictionaries of info from Salesforce for all alumni from the
    campus given, in a format ready for `elasticsearch.helpers.bulk` action.

    The dictionaries include metadata for Elasticsearch:
        `_op_type`: action to take on document, eg. 'create', 'update', etc
        `_index`  : index ('alumni') to which the document belongs
        `_type`   : document type ('alum')
        `_id`     : document's id/pk; set to the alum's CPS/Network ID
    And finally the document itself:
        `_source` : alum-specific data

    Arguments:

    * campus: name of the campus from which to generate alumni; see the
              `CAMPUS_SF_IDS` lookup for valid parameters.

    Available kwargs:

    * action: action to take in elasticsearch (create, update, delete, index)
    """
    # share connection?
    sf_connection = get_salesforce_connection()
    alumni_query = (
        "SELECT Safe_Id__c, Network_Student_ID__c, LastName, "
        "FirstName, Name, HS_Class__c, Facebook_ID__c, OwnerId "
        "FROM Contact "
        "WHERE AccountID = '{}'"
    )

    campus_sf_id = CAMPUS_SF_IDS[campus] # or just fail
    results = sf_connection.query(alumni_query.format(campus_sf_id))

    counts = 0 # spit out at end?
    while True:
        for record in results["records"]:
            counts += 1
            # use empty string where no Facebook ID on file in Salesforce;
            # uniqueness not enforced in Elasticsearch
            facebook_id = record["Facebook_ID__c"] or ""
            source = {
                "safe_id": record["Safe_Id__c"],
                "campus": campus,
                "last_name": record["LastName"],
                "first_name": record["FirstName"],
                "full_name": record["Name"],
                "class_year": int(record["HS_Class__c"]),
                "facebook_id": facebook_id,
                "ac_safe_id": record["OwnerId"],
            }
            new_document = {
                "_op_type": action,
                "_index": ALUMNI_INDEX,
                "_type": ALUMNI_DOC_TYPE,
                "_id": int(record["Network_Student_ID__c"]),
                "_source": source,
            }

            yield new_document

        if not results["done"]:
            results = sf.query_more(results["nextRecordsUrl"], True)
        else:
            print("{:>15}: {}".format(campus, counts))
            break


def _ensure_alumni_index():
    """Make sure it exists, along with the mapping for the alum objects, and
    create aliases to campuses so that they are accessed as if they were
    separate indices.

    CPS/Network ID will be the object's PK in Elastic, so a dedicated field is
    not added to Alum.
    """
    alumni_index = Index(ALUMNI_INDEX)

    # Build mapping for object type.
    # NB. can also be used as class decorator when defining the DocType
    @alumni_index.doc_type
    class Alum(DocType):
        campus = Text() # also used to create aliases as fake indices
        last_name = Text()
        first_name = Text()
        full_name = Text()
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

    _create_campus_aliases()


def _create_campus_aliases():
    """Create an alias in the 'Alumni' index for each campus, allowing a
    pseudo-indexing API; eg. /alumni/alum/<campus> etc.. -> /<campus>/alum/..
    """
    for campus in CAMPUS_SF_IDS.keys():
        _add_es_alias(campus)


def _add_es_alias(campus_name):
    """Adds an alias to ES for the campus_name, so that Alum documents with
    campus=campus_name can be accessed via psuedo-index API,
    eg. makes /<campus>/alum/... possible.
    """
    put_data = {
        "routing": campus_name,
        "filter": {
            "term": {
                "campus": campus_name,
            }
        }
    }
    dest = f"{ES_KEY}/alumni/_alias/{campus_name}"
    r = requests.put(dest, json=put_data)
    r.raise_for_status()


def create_alumni_index():
    """Creates the alumni index in Elasticsearch, aliases the campuses as fake
    indices, and populates the index with alumni data.
    """
    es_connection = es_connections.create_connection(hosts=[ES_KEY], timeout=20)
    _ensure_alumni_index()
    for campus_name in CAMPUS_SF_IDS.keys():
        bulk_gen = _bulk_alum_gen(campus_name, action="create")
        es_bulk_action(es_connection, bulk_gen)


if __name__ == "__main__":
    create_alumni_index()
