#! /usr/bin/python
from lxml import etree
import os
from glob import glob
import json
import sys
sys.path.append("/Users/dykex005/Workspace/borchert-github/metadata-tracking/scripts/OGP-metadata-py/src")
import ogp2solr
from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
from collections import OrderedDict
import pdb
CSW_URL = "http://45.55.225.162/geonetwork/srv/eng/csw-{institution}"
#CSW_URL = "https://lib-geonetdev.oit.umn.edu/geonetwork/srv/eng/csw-{institution}"

institutions_test = {
    "minn":'"University of Minnesota"'
}
institutions = {
    "iowa":'"University of Iowa"',
    "illinois":'"University of Illinois"',
    "minn":'"University of Minnesota"',
    "psu":'"Penn State University"',
    "msu":'"Michigan State University"',
    "mich":'"University of Michigan"',
    "purdue":'"Purdue University"',
    "umd":'"University of Maryland"',
    "wisc":'"University of Wisconsin-Madison"'
}

XSLT_PATH = os.path.join(".","iso2geoBL.xsl")
transform = etree.XSLT(etree.parse(XSLT_PATH))
json_files = glob(os.path.join("geobl", "*.json"))
def clear_dir():
    for i in json_files:
        os.remove(i)
clear_dir()

#s = ogp2solr.SolrOGP(url="http://localhost:8983/solr/blacklight-core/")
s = ogp2solr.SolrOGP(url="http://45.55.44.228:8983/solr/blacklight-core/")

for inst in institutions_test:
    url = CSW_URL.format(institution=inst)
    csw_i = csw.CatalogueServiceWeb(url)
    records = OrderedDict()
    start_pos = 0
    csw_i.getrecords2(esn="full", startposition=start_pos, maxrecords=20,outputschema="http://www.isotc211.org/2005/gmd")
    print inst, csw_i.results
    records.update(csw_i.records)
    while csw_i.results['nextrecord'] <= csw_i.results['matches']:
        start_pos = csw_i.results['nextrecord'] - 1
        csw_i.getrecords2(esn="full", startposition=start_pos, maxrecords=20,outputschema="http://www.isotc211.org/2005/gmd")
        print inst, csw_i.results
        records.update(csw_i.records)
        # for r in csw_i.records:
        #     rec = csw_i.records[r].xml
        #     rec = rec.replace("\n","")
        #     root = etree.fromstring(rec)
        #     record_etree = etree.ElementTree(root)
        #     result = transform(record_etree, institution=institutions[inst])
        #     out_file = open(os.path.join("geobl", r + ".json"), "wb")
        #     out_file.write(result)
        #     out_file.close()
    for r in records:
        rec = records[r].xml
        rec = rec.replace("\n","")
        root = etree.fromstring(rec)
        record_etree = etree.ElementTree(root)
        result = transform(record_etree, institution=institutions[inst])
        out_file = open(os.path.join("geobl", r + ".json"), "wb")
        out_file.write(result)
        out_file.close()

s.delete_query("dct_provenance_s:" + institutions[inst], no_confirm=True)
# s.delete_query("*:*")

json_files = glob(os.path.join("geobl", "*.json"))

for i in json_files:
    print i
    s.add_json_to_solr(i)
