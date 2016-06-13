from lxml import etree
from GeonamesRdf import geonames
import GeonamesRdf.geonames.config
from GeonamesRdf.geonames.adapters import search
GeonamesRdf.geonames.config.IS_DEBUG = True

_USERNAME = raw_input('Enter GeoNames username (or "demo", which may not work): ')

root_tree = etree.parse("gn-blank.rdf")
root_element = root_tree.getroot()
nodes = []

NSMAP = {"cc":"http://creativecommons.org/ns#",
"dcterms":"http://purl.org/dc/terms/",
"foaf":"http://xmlns.com/foaf/0.1/",
"gn":"http://www.geonames.org/ontology#",
"owl":"http://www.w3.org/2002/07/owl#",
"rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
"rdfs":"http://www.w3.org/2000/01/rdf-schema#",
"wgs84_pos":"http://www.w3.org/2003/01/geo/wgs84_pos#",
"skos":"http://www.w3.org/2004/02/skos/core#",
"xml":"http://www.w3.org/XML/1998/namespace"}

alphabet = ["A",
"B",
"C",
"D",
"E",
"F",
"G",
"H",
"I",
"J",
"K",
"L",
"M",
"N",
"O",
"P",
"Q",
"R",
"S",
"T",
"U",
"V",
"W",
"X",
"Y",
"Z"
]

num_rows = 1000

for letter in alphabet:
    sa = geonames.adapters.search.Search(_USERNAME)
    base_q = sa.force_matching_name().language("en").place_name_language("en").country('US').max_rows(num_rows).feature_class("A").feature_code("ADM1").verbosity("FULL").order_by("relevance")
    result = base_q.place_name_starts_with(letter).execute()
    r = result.get_xml_nodes()

    for i in r:
        i.tag = "{http://www.w3.org/2004/02/skos/core#}Concept"
        gn_name = i.findtext("gn:name", namespaces=NSMAP)
        if gn_name.startswith(letter):
            print(gn_name)
            label_elem = etree.Element("{"+NSMAP["skos"]+ "}prefLabel")
            label_elem.set("{"+NSMAP["xml"]+"}lang",'en')
            label_elem.text = gn_name
            i.append(label_elem)
            root_element.append(i)


root_tree.write("gn-states.rdf")
