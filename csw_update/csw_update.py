from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
import logging
import os
import sys
import json
from datetime import datetime
import unicodecsv as csv
import pdb
from config import CSW_URL, USER, PASSWORD

#logging stuff
log = logging.getLogger('owslib')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(log_formatter)
log.addHandler(ch)


class UpdateCSW(object):
    def __init__(self, url, username, password, input_csv_path):
        self.INNER_DELIMITER = "###"
        self.csw = csw.CatalogueServiceWeb(url, username=username, password=password)
        self.records = {}

        if not os.path.isabs(input_csv_path):
            input_csv_path = os.path.abspath(os.path.relpath(input_csv_path, os.getcwd()))

        self.reader = csv.DictReader(open(input_csv_path,"rU"))
        self.fieldnames = self.reader.fieldnames
        self.fieldnames.remove("uuid")
        self.namespaces = self.get_namespaces()
        self.namespaces_no_empty = self.namespaces.copy()
        self.namespaces_no_empty.pop(None)

        # these are the column names that will trigger a change
        self.field_handlers = {"iso19139": {
                "NEW_title": self.NEW_title,
                "NEW_link_download": self.NEW_link_download,
                "NEW_link_service_wms": self.NEW_link_service_wms,
                "NEW_link_service_esri": self.NEW_link_service_esri,
                "NEW_link_information": self.NEW_link_information,
                "NEW_contact_organization": self.NEW_contact_organization,
                "NEW_contact_individual": self.NEW_contact_individual,
                "NEW_topic_categories": self.NEW_topic_categories,
                "NEW_abstract": self.NEW_abstract,
                "NEW_keywords_theme": self.NEW_keywords_theme,
                "NEW_keywords_place": self.NEW_keywords_place
            },
            "dublin-core": {
                u"NEW_title": self.NEW_title,
                u"NEW_abstract": self.NEW_abstract
            }

        }
        self.XPATHS = {"iso19139":{
                "title"                           : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString",
                "date_creation"                   : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='creation']",
                "date_publication"                : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='publication']",
                "date_revision"                   : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='revision']",
                "contact_organization"            : "gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString",
                "contact_individual"              : "gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString",
                "timestamp"                       : "gmd:dateStamp",
                "online_resources"                : "*//gmd:CI_OnlineResource",
                "distribution_link"               : "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions[{index}]/gmd:MD_DigitalTransferOptions[1]/gmd:onLine[1]/gmd:CI_OnlineResource[1]/gmd:linkage[1]/gmd:URL[1]",
                "distributor_distribution_link"   : "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor[{distributor_index}]/gmd:MD_Distributor/gmd:distributorTransferOptions[{index}]/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL",
                "keywords_theme"                  :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='theme']/../../gmd:keyword/gco:CharacterString",
                "keywords_place"                  :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='place']/../../gmd:keyword/gco:CharacterString",
                "topic_categories"                : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode",
                "abstract"                        :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString",
                "purpose"                         :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:purpose/gco:CharacterString",
                "temporalextent_period_start"     :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition",
                "temporalextent_period_end"       :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition",
                "temporalextent_instant"          :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition"
            },
            "dublin-core": {
                "title": "dc:title"
            }

        }
        self.protocol_map = {
            "download": ["WWW:DOWNLOAD-1.0-http--download","download", "WWW:DOWNLOAD"],
            "information": ["WWW:LINK-1.0-http--link", "WWW:LINK"],
            "esri_service": ["ESRI:ArcGIS"],
            "wms_service": ["OGC:WMS"],
            "wfs_service": ["OGC:WFS"],
            "wcs_service": ["OGC:WCS"]
        }

        self.topic_categories = ['intelligenceMilitary', 'environment',
            'geoscientificinformation', 'elevation', 'utilitiesCommunications',
             'structure', 'oceans', 'planningCadastre', 'inlandWaters', 'boundaries',
             'society', 'biota', 'health', 'location', 'climatologyMeteorologyAtmosphere',
             'transportation', 'farming', 'imageryBaseMapsEarthCover', 'economy']


    @staticmethod
    def get_namespaces():
        n = Namespaces()
        ns = n.get_namespaces(["gco","gmd","gml","gml32","gmx","gts","srv","xlink","dc"])
        ns[None] = n.get_namespace("gmd")
        return ns


    @staticmethod
    def _filter_link_updates(field):
        if field.startswith("NEW_link"):
            return True


    def _get_links_from_record(self, uuid):
        """
        Sets self.record_online_resources to a list of all CI_OnlineResource elements
        """

        self._get_etree_for_record(uuid)
        self.record_online_resources = self.record_etree.findall(self.XPATHS[self.schema]["online_resources"], self.namespaces)


    def _get_etree_for_record(self, uuid):
        """
        Set self.record_etree to etree ElementTree of record with inputted uuid.
        """

        xml = self.records[uuid].xml
        root = etree.fromstring(xml)
        self.record_etree = etree.ElementTree(root)


    def _check_for_links_to_update(self, link_type):
        """
        Return a list of links that match a given type (download, information,
        esri_service, wms_service are current values for link_type)
        """
        protocols_list = self.protocol_map[link_type]
        links_to_update = filter(lambda resource,
            ns=self.namespaces,
            protocols=protocols_list: resource.findtext("gmd:protocol/gco:CharacterString", namespaces=ns) in protocols_list,
            self.record_online_resources
            )

        return links_to_update

    def _simple_element_update(self,uuid, element, new_value):
        """
        Updates single element of record
        """
        pn = self.XPATHS[self.schema][element]
        tt = "update"
        tn ="csw:Record"
        pv = new_value
        ident = str(uuid)

        t = self.csw.transaction(ttype=tt,
            typename=tn,
            propertyname=pn,
            propertyvalue=pv,
            identifier=ident)

        log.debug(self.csw.request)
        log.debug(self.csw.response)

    def NEW_abstract(self, uuid, new_abstract):
        """
        Updates abstract of record
        """
        self._simple_element_update(uuid, "abstract", new_abstract)

    def NEW_title(self, uuid, new_title):
        """
        Updates title of record
        """
        self._simple_element_update(uuid, "title", new_title)


    def _update_links(self, uuid, new_link, link_type):
        """
        Base function for updating links
        """
        links_to_update = self._check_for_links_to_update(link_type)

        for i in links_to_update:
            elem = i.find("gmd:linkage/gmd:URL", namespaces=self.namespaces)
            current_val = elem.text

            if current_val and current_val == new_link:
                log.info("Value is already set to {link}. Skipping!".format(link=new_link))
                continue

            xpath = self.record_etree.getpath(elem)
            xpath = "/".join(xpath.split("/")[2:])

            log.debug("_update_links XPATH: " + xpath)

            self.csw.transaction(ttype="update",
                typename='csw:Record',
                propertyname=xpath,
                propertyvalue=new_link,
                identifier=uuid)

            log.debug(self.csw.request)
            log.debug(self.csw.response)

    def NEW_link_download(self, uuid, new_link):
        self._update_links(links, uuid, new_link, "download")


    def NEW_link_service_esri(self, uuid, new_link):
        self._update_links(links, uuid, new_link, "esri_service")


    def NEW_link_service_wms(self, uuid, new_link):
        self._update_links(links, uuid, new_link, "wms_service")


    def NEW_link_information(self, uuid, new_link):
        self._update_links(links, uuid, new_link, "information")

    def _make_new_topic_element(self, cat_text):
        p = etree.Element("{gmd}topicCategory".format(gmd="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        c = etree.SubElement(p,"{gmd}MD_TopicCategoryCode".format(gmd="{"+self.namespaces["gmd"]+"}"))
        c.text = cat_text
        return p


    def NEW_topic_categories(self, uuid, new_topic_categories):
        """
        This is heinous. I'm sorry.
        """
        cat_list = new_topic_categories.split(self.INNER_DELIMITER)
        log.debug("NEW TOPIC INPUT: " + new_topic_categories)

        if len(cat_list) == 1 and cat_list[0] == "":
            return

        self.get_record_by_id(uuid)
        self._get_etree_for_record(uuid)
        tree = self.record_etree
        tree_changed = False
        xpath = self.XPATHS[self.schema]["topic_categories"]
        existing_cats = tree.findall(xpath, namespaces=self.namespaces)
        existing_cats_text = [i.text for i in existing_cats ]

        log.debug("EXISTING CATEGORIES: " + ", ".join(existing_cats_text))

        new_cats = list(set(cat_list) - set(existing_cats_text))
        delete_cats = list(set(existing_cats_text)-set(cat_list))

        log.debug("NEW CATEGORIES: " + ", ".join(new_cats))
        log.debug("CATEGORIES TO DELETE: " + ", ".join(delete_cats))

        for cat_text in new_cats:

            if cat_text not in self.topic_categories:
                log.warn("Invalid topic category not added: " + cat_text)
                continue

            new_cat_element = self._make_new_topic_element(cat_text)
            existing_cats[-1].addnext(new_cat_element)
            tree_changed = True

        for delete_cat in delete_cats:
            del_ele = tree.xpath("//gmd:MD_TopicCategoryCode[text()='{cat}']".format(cat=delete_cat), namespaces=self.namespaces_no_empty)
            if len(del_ele) == 1:
                p = del_ele[0].getparent()
                p.remove(del_ele[0])
                pp = p.getparent()
                pp.remove(p)
                tree_changed = True

        if tree_changed:
            t = self.csw.transaction(ttype="update",
                typename='csw:Record',
                record=etree.tostring(tree),
                identifier=uuid)

            log.debug(self.csw.results)

    def NEW_keywords_place(self, uuid, new_keywords):
        self._multiple_element_update(uuid, new_keywords, "keywords_place")

    def NEW_keywords_theme(self, uuid, new_keywords):
        self._multiple_element_update(uuid, new_keywords, "keywords_theme")

    def _get_record_and_etree(self, uuid):
        self.get_record_by_id(uuid)
        self._get_etree_for_record(uuid)
        return self.record_etree

    def _make_new_multiple_element(self, element_name, value):
        #TODO abstract beyond keywords using element_name

        element = etree.Element("{ns}keyword".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        child_element = etree.SubElement(element,"{ns}CharacterString".format(ns="{"+self.namespaces["gco"]+"}"))
        child_element.text = value
        return element

    def _multiple_element_update(self, uuid, new_values_string, multiple_element_name):
        """
        This is heinous. I'm sorry.
        """
        log.debug("NEW VALUE INPUT: " + new_values_string)

        new_values_list = new_values_string.split(self.INNER_DELIMITER)

        if len(new_values_list) == 1 and new_values_list[0] == "":
            return

        tree = self._get_record_and_etree(uuid)
        tree_changed = False
        xpath = self.XPATHS[self.schema][multiple_element_name]
        existing_values = tree.findall(xpath, namespaces=self.namespaces)
        existing_values_text = [i.text for i in existing_values]

        log.debug("EXISTING VALUES: " + ", ".join(existing_values_text))

        add_values = list(set(new_values_list) - set(existing_values_text))
        delete_values = list(set(existing_values_text) - set(new_values_list))

        log.debug("VALUES TO ADD: " + ", ".join(add_values))
        log.debug("VALUES TO DELETE: " + ", ".join(delete_values))

        for delete_value in delete_values:
            #TODO abstract out keyword specifics
            del_ele = tree.xpath("//gmd:keyword/gco:CharacterString[text()='{val}']".format(val=delete_value), namespaces=self.namespaces_no_empty)
            if len(del_ele) == 1:
                import pdb; pdb.set_trace()
                p = del_ele[0].getparent()
                p.remove(del_ele[0])
                pp = p.getparent()
                pp.remove(p)
                tree_changed = True

        for value in add_values:

            # TODO handle specific things like this? maybe another dict of
            # elements that have a controlled vocab?
            # if value not in self.topic_categories:
            #     log.warn("Invalid topic category not added: " + value)
            #     continue

            new_element = self._make_new_multiple_element(multiple_element_name, value)
            existing_values[-1].getparent().addnext(new_element)
            tree_changed = True

        if tree_changed:
            t = self.csw.transaction(ttype="update",
                typename='csw:Record',
                record=etree.tostring(tree),
                identifier=uuid)

            log.debug(self.csw.results)

    def NEW_contact_organization(self):
        #stub
        pass


    def NEW_contact_individual(self):
        #stub
        pass


    def update_timestamp(self, uuid):
        ts = datetime.now().isoformat()
        val = ts
        tree = self._get_record_and_etree(uuid)
        pn = self.XPATHS[self.schema]["timestamp"] + "/gco:DateTime"
        dateStamp = tree.find(pn, namespaces=self.namespaces)

        if dateStamp is None:
            pn = self.XPATHS[self.schema]["timestamp"] + "/gco:Date"
            dateStamp = tree.find(pn, namespaces=self.namespaces)
            val = ts[:10]

        return self.csw.transaction(ttype="update",
            typename='csw:Record',
            propertyname=pn,
            propertyvalue=val,
            identifier=uuid)


    def get_record_by_id(self, uuid):

        if self.records.has_key(uuid):
            return
        elif self.schema == "iso19139":
            outschema = self.get_namespaces()["gmd"]
            self.csw.getrecordbyid(id=[str(uuid)], outputschema=outschema)
            self.records[uuid] = self.csw.records[uuid]
        else:
            outschema = "http://www.opengis.net/cat/csw/2.0.2"
            xml_text = """<csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:ogc="http://www.opengis.net/ogc" service="CSW" version="2.0.2" resultType="results" startPosition="1" maxRecords="10" outputFormat="application/xml" outputSchema="http://www.opengis.net/cat/csw/2.0.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:apiso="http://www.opengis.net/cat/csw/apiso/1.0">
              <csw:Query typeNames="csw:Record">
                <csw:ElementSetName>full</csw:ElementSetName>
                <csw:Constraint version="1.1.0">
                  <ogc:Filter>
                    <ogc:PropertyIsLike matchCase="false" wildCard="%" singleChar="_" escapeChar="\">
                      <ogc:PropertyName>dc:identifier</ogc:PropertyName>
                      <ogc:Literal>{id}</ogc:Literal>
                    </ogc:PropertyIsLike>
                  </ogc:Filter>
                </csw:Constraint>
              </csw:Query>
            </csw:GetRecords>""".format(id=uuid)
            self.csw.getrecords2(xml=xml_text)
            self.records[uuid] = self.csw.records.items()[0][1]

    def process_spreadsheet(self):
        for row in self.reader:

            #TODO add check for NEW_topic_categories or NEW_keywords_* that will
            #prevent transaction until all updates made to local etree, which may
            #slow things down. Maybe not.

            #log.debug(row)
            row_changed = False
            self.uuid = row["uuid"]
            self.schema = row["schema"]

            if len(filter(self._filter_link_updates, self.fieldnames)) > 0:
                self.get_record_by_id(self.uuid)
                self._get_links_from_record(self.uuid)

            for field in self.fieldnames:

                if field in self.field_handlers[self.schema] and row.has_key(field):

                    row_changed=True
                    self.field_handlers[self.schema][field].__call__(self.uuid, row[field])

            # TODO need to make change detection much more sophisticated (right now it updates timestamp even if it doesn't actually alter values)
            # also it adds datetime as timestamp, even if there's just a date, resulting to invalidating the record bc of two elements w/in timestamp
            if row_changed:
               self.update_timestamp(self.uuid)



f = UpdateCSW(CSW_URL, USER, PASSWORD, "sample_csv/CSWUpdateTest_keywords3.csv")

f.process_spreadsheet()
