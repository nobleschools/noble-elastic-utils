"""
create_fb_ignore_index.py

Create an Elasticsearch index for the Facebook contacts that are known to
not be alumni, and thus should be ignored when processing FB notes.
"""

import csv
from datetime import datetime

from elasticsearch.helpers import bulk as es_bulk_action
from elasticsearch_dsl import DocType, Index, Text
from elasticsearch_dsl.connections import connections as es_connections

from secrets.elastic_secrets import ES_CONNECTION_KEY as ES_KEY


def create_fb_ignore_index():
    """
    Create a 'fb-ignore' index, where NonAlumContact documents will hold
    Facebook Name and Facebook ID for contacts that will be ignored when
    importing contact notes from Facebook conversations.
    """
    # start with one
    fb_ignores_index = Index("fb-ignore")

    # build mapping for object type
    # can also be used as class decorator when defining the DocType
    @fb_ignores_index.doc_type
    class NonAlumContact(DocType):
        facebook_name = Text()
        facebook_id = Text()

    # save the index
    # bug in error handling (due to version mismatch? https://github.com/elastic/elasticsearch-py/issues/496)
    try:
        fb_ignores_index.create(ignore=400)
    except AuthenticationException as e:
        print(e.error)
        raise e


def fb_ignores_gen():
    """
    Generator to feed the `elasticsearch.helpers.bulk` function.
    """
    # ES bulk creates don't support auto-id from version 5+
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking_50_index_apis.html#_optype_create_without_an_id
    document_id = 0

    with open("matched_ignores.csv") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            document_id += 1
            # come from Facebook in format '<id>@facebook.com';
            # use empty string otherwise
            facebook_id = ""
            if row["Facebook ID"]:
                facebook_id = row["Facebook ID"].split("@")[0]

            yield {
                "_op_type": "create",
                "_index"  : "fb-ignore",
                "_type"   : "non-alum-contact",
                "_id"     : document_id,
                "_source" : {
                    "facebook_name": row["Facebook Name"],
                    "facebook_id"  : facebook_id,
                },
            }


def create_fb_ignore_indices():
    """
    Create index in Elasticsearch, and populate with data from
    'matched_ignores.csv' file.
    """
    es_connection = es_connections.create_connection(hosts=[ES_KEY], timeout=20)
    create_fb_ignore_index()

    bulk_gen = fb_ignores_gen()
    es_bulk_action(es_connection, bulk_gen)


if __name__ == "__main__":
    create_fb_ignore_indices()
