from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from pprint import pprint

#URL = 'http://quod.lib.umich.edu/cgi/o/oai/oai'
URL = 'http://umedia.lib.umn.edu/oai2'
registry = MetadataRegistry()
registry.registerReader('oai_dc', oai_dc_reader)
client = Client(URL, registry)
records = []
for record in client.listRecords(metadataPrefix='oai_dc'):
    records.append(record)
    print record
