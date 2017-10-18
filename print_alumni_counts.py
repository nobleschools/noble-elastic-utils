"""
print_alumni_counts.py

Prints the total alumni counts in Salesforce (from a particular year back) and
Elasticsearch.
"""

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from elasticsearch_dsl import (
    Index,
    Search,
)
from elasticsearch_dsl.connections import connections as es_connections

from salesforce_utils.constants import CAMPUS_SF_IDS
from salesforce_utils.get_connection import get_salesforce_connection
from secrets.elastic_secrets import ES_CONNECTION_KEY as ES_KEY


def print_alumni_counts():

    es_total = sf_total = 0

    for campus_name, campus_sf_id in CAMPUS_SF_IDS.items():
        count_query = (
            f"SELECT COUNT() FROM Contact WHERE AccountID = '{campus_sf_id}' "
        )
        salesforce_campus_count = sf_connection.query(count_query)['totalSize']
        sf_total += salesforce_campus_count
        elastic_campus_count = Search(index=campus_name).count()
        es_total += elastic_campus_count
        print("{0}\n    SF:{1:>5}\n    ES:{2:>5}".format(
            campus_name, salesforce_campus_count, elastic_campus_count
        ))

    print('-'*10, '\n')
    print("total\n    SF:{:>5}\n    ES:{:>5} (alumni)".format(
        sf_total, es_total
    ))


if __name__ == "__main__":
    sf_connection = get_salesforce_connection()
    es_connection = es_connections.create_connection(hosts=[ES_KEY], timeout=20)
    print_alumni_counts()
