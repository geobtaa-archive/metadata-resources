import requests
import os
import pdb
from lxml import etree
import glob

ISO_MD_PATH = "/Users/dykex005/Workspace/CIC-GDDP/edu.umn/Minnesota_Geospatial_Commons/2_metadata_transition"

XSLT_PATH = "add_topic_category.xsl"
xslt_root = etree.parse(XSLT_PATH)
transform = etree.XSLT(xslt_root)

xmls = glob.glob(os.path.join(ISO_MD_PATH,"*.xml"))

TOPIC_CATEGORY_MAP = {"biota":"biota",
    "bdry":"boundaries",
    "agri":"farming",
    "atmos":"climatologyMeteorologyAtmosphere",
    "econ":"economy",
    "env":"environment",
    "health":"health",
    "base":"imageryBaseMapsEarthCover",
    "water":"inlandWaters",
    "loc":"location",
    "plan":"planningCadastre",
    "society":"society",
    "struc":"structure",
    "trans":"transportation",
    "util":"utilitiesCommunication",
    "ocean":"oceans",
    "milit":"intelligenceMilitary",
    "geos":"geoscientificInformation",
    "elev":"elevation"}

for xml in xmls:
    metadata = etree.parse(xml)
    tc = False
    for topic in TOPIC_CATEGORY_MAP:
        if "_" + topic + "_" in xml:
            tc = "'" + TOPIC_CATEGORY_MAP[topic] + "'"

    if tc:
        result_metadata = transform(metadata, topic_category=tc)
        result_metadata.write(xml, pretty_print=True)
