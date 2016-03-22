from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from pprint import pprint

URL = 'http://quod.lib.umich.edu/cgi/o/oai/oai'

registry = MetadataRegistry()
registry.registerReader('oai_dc', oai_dc_reader)
client = Client(URL, registry)
for record in client.listRecords(metadataPrefix='oai_dc'):
    r = record[1]
    m = r.getMap()
    for i in m:
        print(i, " : ",r.getField(i))