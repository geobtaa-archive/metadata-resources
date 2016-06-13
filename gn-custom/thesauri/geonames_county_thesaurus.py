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
    base_q = sa.force_matching_name().place_name_language("en").country('US').max_rows(num_rows).feature_class("A").feature_code("ADM2").verbosity("FULL").order_by("relevance")
    result = base_q.place_name_starts_with(letter).execute()
    r = result.get_xml_nodes()
    nodes = nodes + r
    print letter,len(r)

for i in nodes:
    print(i)
    i.tag = "{http://www.w3.org/2004/02/skos/core#}Concept"
    gn_name = i.findtext("gn:name", namespaces=NSMAP)
    wiki_article = i.find("gn:wikipediaArticle", namespaces=NSMAP)
    state_name_maybe = None
    if wiki_article is not None:
        url = wiki_article.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
        if url:
            state_name_maybe = url.split("_")[-1]
    if state_name_maybe:
        label = gn_name + ", " + state_name_maybe
        label_elem = etree.Element("{"+NSMAP["skos"]+ "}prefLabel")
        label_elem.set("{"+NSMAP["xml"]+"}lang",'en')
        label_elem.text = label
        i.append(label_elem)

    root_element.append(i)

root_tree.write("gn-counties.rdf")
