import requests
import os
import pdb
from lxml import etree

lyrs = []
resources = []
START_PATH = "/Users/dykex005/metadata/mn-geospatial-commons"
ISO_MD_PATH = "/Users/dykex005/Workspace/CIC-GDDP/edu.umn/Minnesota_Geospatial_Commons/2_metadata_transition"
ONLINE_RESOURCE_XSLT_PATH = "/Users/dykex005/Workspace/CIC-GDDP/metadata-resources/add_online_resource.xsl"

#build list of all lyr_text files
for path, dirs, files in os.walk(START_PATH):
    if "lyr_text.txt" in files:
        resources.append(os.path.split(path)[-1])
        lyrs.append(os.path.join(path, "lyr_text.txt"))

xslt_root = etree.parse(ONLINE_RESOURCE_XSLT_PATH)
transform = etree.XSLT(xslt_root)
for index, res in enumerate(resources):
    metadata = etree.parse(os.path.join(ISO_MD_PATH, res + "_iso.xml"))
    lyr_text = open(os.path.join(START_PATH,res,"lyr_text.txt"))
    lyr_info = lyr_text.readlines()
    for lyr in lyr_info:
        layer = lyr.split("|||")
        lyr_type = layer[0]
        url = layer[1].rstrip("\n")
        protocol = "ESRI:ArcGIS"
        if lyr_type == "MapService":
            description = "DynamicMapLayer"
        elif lyr_type.startswith("Tiled"):
            description = "TiledMapLayer"
        elif lyr_type.startswith("Feature"):
            description = "FeatureLayer"
        else:
            description = "DynamicMapLayer"

        result_metadata = transform(metadata, description="'"+description+"'",url="'"+url+"'", protocol="'"+protocol+"'")
        result_metadata.write(os.path.join(ISO_MD_PATH, res + "_iso.xml"))