===================
Noble Elastic Utils
===================
Alumni data cached in Elasticsearch (hosted on Bonsai as Heroku add-on),
primarily mirroring Salesforce data for search purposes.

Modules
-------

create_alumni_index.py
======================

Creates a shared Elasticsearch index ('alumni') to hold alumni data, and
populates it with data from Salesforce.

Creates `fake indices`_ for each campus using aliases, to skirt around the index
limit on our current tier on Bonsai.

.. _fake indices: https://www.elastic.co/guide/en/elasticsearch/guide/current/faking-it.html

