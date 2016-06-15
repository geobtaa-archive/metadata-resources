#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

# Python standard libs
import logging
import os
import sys
import json
from datetime import datetime
import argparse
import time
import pdb

# non standard dependencies
from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
import unicodecsv as csv
from dateutil import parser

# config options - see config.py.sample for how to structure
from config import CSW_URL, USER, PASSWORD, DEBUG

#logging stuff
if DEBUG:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
log = logging.getLogger('owslib')
log.setLevel(log_level)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(log_level)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(log_formatter)
log.addHandler(ch)

class UpdateCSW(object):
    def __init__(self, url, username, password, input_csv_path):
        self.INNER_DELIMITER = "###"
        self.GEMET_ANCHOR_BASE_URI = "https://geonet.lib.umn.edu:80/geonetwork/srv/eng/xml.keyword.get?thesaurus=external.theme.gemet-en&id="
        self.csw = csw.CatalogueServiceWeb(url, username=username, password=password)
        self.records = {}

        if not os.path.isabs(input_csv_path):
            input_csv_path = os.path.abspath(os.path.relpath(input_csv_path, os.getcwd()))

        self.csvfile = open(input_csv_path,"rU")

        self.reader = csv.DictReader(self.csvfile)
        self.fieldnames = self.reader.fieldnames

        self.namespaces = self.get_namespaces()

        # for etree.xpath function we can't have an empty namespace (None)
        self.namespaces_no_empty = self.namespaces.copy()
        self.namespaces_no_empty.pop(None)

        # these are the column names that will trigger a change
        self.field_handlers = {"iso19139": {
                "NEW_title": self.NEW_title,
                "NEW_link_download": self.NEW_link_download,
                "NEW_link_service_wms": self.NEW_link_service_wms,
                "NEW_link_service_esri": self.NEW_link_service_esri,
                "NEW_link_information": self.NEW_link_information,
                "NEW_distribution_format": self.NEW_distribution_format,
                "NEW_contact_organization": self.NEW_contact_organization,
                "NEW_contact_individual": self.NEW_contact_individual,
                "NEW_topic_categories": self.NEW_topic_categories,
                "NEW_abstract": self.NEW_abstract,
                "NEW_keywords_theme": self.NEW_keywords_theme,
                "NEW_keywords_theme_gemet_name": self.NEW_keywords_theme_gemet_name,
                "NEW_keywords_place": self.NEW_keywords_place,
                "NEW_keywords_place_geonames": self.NEW_keywords_place_geonames,
                "NEW_date_publication": self.NEW_date_publication,
                "NEW_date_revision": self.NEW_date_revision,
                "NEW_temporal_end": self.NEW_temporal_end,
                "NEW_temporal_start": self.NEW_temporal_start,
                "NEW_temporal_instant": self.NEW_temporal_instant,
                "DELETE_link": self.DELETE_link,
                "DELETE_link_no_protocol": self.DELETE_link_no_protocol
            },

            #currently unused
            "dublin-core": {
                u"NEW_title": self.NEW_title,
                u"NEW_abstract": self.NEW_abstract
            }

        }
        self.XPATHS = {"iso19139":{
                "citation"                        : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation",
                "title"                           : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString",
                "distribution_format"             : "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format/gmd:name/gco:CharacterString",
                "date_creation"                   : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='creation']",
                "date_publication"                : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='publication']",
                "date_revision"                   : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='revision']",
                "contact_organization"            : "gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString",
                "contact_individual"              : "gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString",
                "timestamp"                       : "gmd:dateStamp",
                "md_distribution"                 : "gmd:distributionInfo/gmd:MD_Distribution",
                "transferOptions"                 : "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions",
                "digital_trans_options"           : "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions",
                "online_resources"                : "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource",
                "online_resource_links"           : "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL",
                "link_no_protocol"                : "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource[not(gmd:protocol)]",
                "distribution_link"               : "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions[{index}]/gmd:MD_DigitalTransferOptions[1]/gmd:onLine[1]/gmd:CI_OnlineResource[1]/gmd:linkage[1]/gmd:URL[1]",
                "distributor_distribution_link"   : "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor[{distributor_index}]/gmd:MD_Distributor/gmd:distributorTransferOptions[{index}]/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL",
                "keywords_theme"                  :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='theme']/../../gmd:keyword/gco:CharacterString",
                "keywords_theme_gemet"            :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString[text()='GEMET']",
                "keywords_place"                  :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='place']/../../gmd:keyword/gco:CharacterString",
                "descriptive_keywords"            :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords",
                "md_data_identification"          :"gmd:identificationInfo/gmd:MD_DataIdentification",
                "topic_categories"                : "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode",
                "abstract"                        :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString",
                "purpose"                         :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:purpose/gco:CharacterString",
                "extent"                          :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent",
                "temporalextent"                  :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement",
                "temporalextent_start"     :"gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition",
                "temporalextent_end"       :"gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition",
                "temporalextent_instant"          :"gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition"
                #"link_information"                :"gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:protocol/gco:CharacterString[text()='WWW:LINK'"
            },

            "dublin-core": {
                "title": "dc:title"
            }

        }
        self.protocol_map = {
            "download": ["WWW:DOWNLOAD-1.0-ftp--download","download","WWW:DOWNLOAD-1.0-http--download","WWW:DOWNLOAD"],
            "information": ["WWW:LINK","WWW:LINK-1.0-http--link"],
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
        """
        Returns specified namespaces using owslib Namespaces function.
        """

        n = Namespaces()
        ns = n.get_namespaces(["gco","gmd","gml","gml32","gmx","gts","srv","xlink","dc"])
        ns[None] = n.get_namespace("gmd")
        return ns


    @staticmethod
    def _filter_link_updates_or_deletions(field):
        """
        Filter function used to identify link updates or deletions from the CSV
        """

        if field.startswith("NEW_link") or field.startswith("DELETE_link"):
            return True


    def _get_links_from_record(self, uuid):
        """
        Sets self.record_online_resources to a list of all CI_OnlineResource elements
        """

        self.record_online_resources = self.record_etree.findall(self.XPATHS[self.schema]["online_resources"], self.namespaces)


    def _simple_element_update(self,uuid, new_value, xpath=None,element=None):
        """
        Updates single element of record. Nothing fancy. Elements like abstract and title.

        Positional arguments:
        uuid -- the unique id of the record to be updated
        new_value -- the new value supplied from the csv

        Keyword arguments (need one and only one):
        xpath -- must follow straight from the root element
        element -- match a name in self.XPATHS for the current schema
        """

        if xpath:
            path = xpath
        elif element:
            path = self.XPATHS[self.schema][element]
        else:
            log.error("_simple_element_update: No xpath or element provided")
            return

        tree = self.record_etree
        elem = tree.find(path, namespaces=self.namespaces)
        if elem is not None:
            if elem.text != new_value:
                elem.text = new_value
                self.tree_changed = True
            else:
                log.info("Value for \n {p} \n already set to: {v}".format(p=path.split("/")[-2],v=new_value))


    def _check_for_links_to_update(self, link_type):
        """
        Return a list of links that match a given type.

        Positional argument:
        link_type -- The type of link to look for. download, information,
            esri_service, and wms_service are current values for link_type)
        """
        self.protocols_list = self.protocol_map[link_type]
        links_to_update = filter(lambda resource,
            ns=self.namespaces,
            protocols=self.protocols_list: resource.findtext("gmd:protocol/gco:CharacterString", namespaces=ns) in self.protocols_list,
            self.record_online_resources
            )
        return links_to_update


    def _add_protocol_to_resource(self, resource, link_type):
        """
        Creates a protocol element and its text for a given online resource.

        Positional arguments:
        resource -- A CI_Online_Resource currently lacking a protocol.
        link_type -- The type of link, which determines the protocol applied.
        """

        protocol_element = etree.SubElement(resource,
            "{ns}protocol".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)
        char_string = etree.SubElement(protocol_element,
            "{ns}CharacterString".format(ns="{"+self.namespaces["gco"]+"}"),
            nsmap=self.namespaces)
        char_string.text = self.protocol_map[link_type][0]
        log.debug("Added protocol: {prot}".format(prot=char_string.text))

        #log.debug(etree.tostring(self.record_etree))
        return resource


    def _update_links_no_protocol(self, new_link, link_type, resources_no_protocol):
        """
        Matches inputted link to existing resources without protocols and if successful, adds protocol.

        Positional arguments:
        new_link -- The link to search for
        link_type -- The type of link, which us be used to create the protocol
        resources_no_protocol -- A list of OnlineResource Elements lacking protocol SubElements
        """

        for resource in resources_no_protocol:
            if resource.find("gmd:linkage/gmd:URL", namespaces=self.namespaces).text == new_link:

                log.debug("updating resource with no protocol")
                self._add_protocol_to_resource(resource, link_type)
                #log.debug(etree.tostring(self.record_etree))
                self.tree_changed = True


    def _create_new_link(self, new_link, link_type):
        """
        Create a new onLine element. Presumes that gmd:MD_DigitalTransferOptions exists.

        Positional arguments:
        new_link -- The link to search for
        link_type -- The type of link, which us be used to create the protocol
        """

        transferOptions = self.record_etree.xpath(self.XPATHS[self.schema]["transferOptions"],
            namespaces=self.namespaces_no_empty)

        if len(transferOptions) > 0:
            digital_trans_options = self.record_etree.xpath(self.XPATHS[self.schema]["digital_trans_options"],
                namespaces=self.namespaces_no_empty)

            if len(digital_trans_options) > 0:

                #create the elements
                online_element = etree.SubElement(digital_trans_options[0],
                    "{ns}onLine".format(ns="{"+self.namespaces["gmd"]+"}"),
                    nsmap=self.namespaces)
                ci_onlineresource = etree.SubElement(online_element,
                    "{ns}CI_OnlineResource".format(ns="{"+self.namespaces["gmd"]+"}"),
                    nsmap=self.namespaces)
                linkage = etree.SubElement(ci_onlineresource,
                    "{ns}linkage".format(ns="{"+self.namespaces["gmd"]+"}"),
                    nsmap=self.namespaces)
                url = etree.SubElement(linkage,
                    "{ns}URL".format(ns="{"+self.namespaces["gmd"]+"}"),
                    nsmap=self.namespaces)
                protocol = etree.SubElement(ci_onlineresource,
                    "{ns}protocol".format(ns="{"+self.namespaces["gmd"]+"}"),
                    nsmap=self.namespaces)
                protocol_string = etree.SubElement(protocol,
                    "{ns}CharacterString".format(ns="{"+self.namespaces["gco"]+"}"),
                    nsmap=self.namespaces)

                #add the text
                url.text = new_link
                protocol_string.text = self.protocols_list[0]

                self.tree_changed = True

                log.debug("created new link: {link}".format(link=new_link))
                #log.debug(etree.tostring(self.record_etree))
                #log.debug(self.csw.response)

        else:
            md_distribution = self.record_etree.xpath(self.XPATHS[self.schema]["md_distribution"],
                namespaces=self.namespaces_no_empty)
            if len(md_distribution) > 0:
                transfer_options = self._create_transferOptions(md_distribution[0])
                self._create_md_digital_transfer_options(transfer_options)

                #recurse and try to make link again now that the parents are in place
                log.debug("trying to create link again")
                self._create_new_link(new_link, link_type)


    def _create_transferOptions(self, md_distribution):
        return etree.SubElement(md_distribution,
            "{ns}transferOptions".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)

    def _create_md_digital_transfer_options(self, transfer_options):
        return etree.SubElement(transfer_options,
            "{ns}MD_DigitalTransferOptions".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)

    def _current_link_url_elements(self):
        return self.record_etree.xpath(self.XPATHS[self.schema]["online_resource_links"],
            namespaces=self.namespaces_no_empty)

    def _current_link_urls(self):
        """
        Return a list of all URLs currently in the record.
        """

        links = self._current_link_url_elements()
        log.debug("Current link urls: " + " | ".join([link.text for link in links]))
        return [link.text for link in links]


    def _get_resources_no_protocol(self):
        return self.record_etree.xpath(self.XPATHS[self.schema]["link_no_protocol"],
            namespaces=self.namespaces_no_empty)

    def _update_links(self, uuid, new_link, link_type):
        """
        Base function for updating links
        """
        tree = self.record_etree
        self._get_links_from_record(uuid)
        record_links = self._current_link_urls()
        #import pdb; pdb.set_trace()
        links_to_update = self._check_for_links_to_update(link_type)
        resources_no_protocol = self._get_resources_no_protocol()
        if len(links_to_update) == 0 and resources_no_protocol is not None:
            self._update_links_no_protocol(new_link,
                link_type,
                resources_no_protocol)

        #log.debug(etree.tostring(self.record_etree))
        for i in links_to_update:
            elem = i.find("gmd:linkage/gmd:URL", namespaces=self.namespaces)
            current_val = elem.text
            current_protocol = i.find("gmd:protocol/gco:CharacterString", namespaces=self.namespaces)
            log.debug("Current protocol: {p}".format(p=current_protocol.text))
            log.debug("Current text: {t}".format(t=current_val))

            if current_protocol.text in self.protocols_list and current_protocol.text != "WWW:DOWNLOAD":

                if current_val and current_val == new_link:
                    #if so, we have nothing to do!
                    log.info("Value is already set to {link}. Skipping!".format(link=new_link))
                    continue

                else:
                    log.debug("Updating link from {old} to {new}".format(old=current_val,
                        new=new_link))
                    elem.text = new_link
                    record_links.append(new_link)
                    record_links.remove(current_val)
                    self.tree_changed = True

                    xpath = self.record_etree.getpath(elem)
                    xpath = "/".join(xpath.split("/")[2:])


            else:
                log.debug("Updating protocol from {old} to {new}".format(old=current_protocol.text,
                    new=self.protocols_list[0]))
                value = self.protocols_list[0]
                current_protocol = value
                self.tree_changed = True

                xpath = self.record_etree.getpath(current_protocol)
                xpath = "/".join(xpath.split("/")[2:])


            # log.debug("_update_links XPATH: " + xpath)
            #
            # self.csw.transaction(ttype="update",
            #     typename='csw:Record',
            #     propertyname=xpath,
            #     propertyvalue=value,
            #     identifier=uuid)
            # time.sleep(2)
            # self.row_changed = True
            #
            # log.debug(self.csw.request)
            # log.debug(self.csw.response)


        #if the new url is nowhere to be found, create a new resource
        if new_link not in record_links:

            log.debug("Current links: " + ", ".join(self._current_link_urls()))
            log.debug("Creating a new link")
            self._create_new_link(new_link, link_type)
            log.debug("Updated links: " + ", ".join(self._current_link_urls()))

    # def _get_record_and_etree(self, uuid):
    #     self.get_record_by_id(uuid)
    #     self.record_etree
    #     return self.record_etree

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

        tree = self.record_etree
        tree_changed = False
        xpath = self.XPATHS[self.schema][multiple_element_name]
        existing_values = tree.findall(xpath, namespaces=self.namespaces)
        existing_values_parent = existing_values[0].getparent().getparent()
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
            existing_values_parent.append(new_element)
            tree_changed = True

        if tree_changed:
            self.tree_changed = True


    def _make_new_topic_element(self, cat_text):
        p = etree.Element("{gmd}topicCategory".format(gmd="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        c = etree.SubElement(p,"{gmd}MD_TopicCategoryCode".format(gmd="{"+self.namespaces["gmd"]+"}"))
        c.text = cat_text
        return p


    def NEW_abstract(self, uuid, new_abstract):
        """
        Updates abstract of record
        """
        if new_abstract != "" and new_abstract != "SKIP":
            update = self._simple_element_update(uuid, new_abstract, element="abstract")
            log.info("updated abstract")

    def NEW_distribution_format(self, uuid, new_format):
        """
        Updates abstract of record
        """
        if new_format != "" and new_format != "SKIP":
            update = self._simple_element_update(uuid, new_format, element="distribution_format")
            log.info("updated distribution format")

    def NEW_title(self, uuid, new_title):
        """
        Updates title of record
        """
        if new_title != "" and new_title != "SKIP":
            update = self._simple_element_update(uuid, new_title, element="title")
            log.info("updated title")


    def NEW_link_download(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "download")
            log.info("updated download link")

    def NEW_link_service_esri(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "esri_service")
            log.info("updated esri_service link")

    def NEW_link_service_wms(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "wms_service")
            log.info("updated wms_service link")

    def NEW_link_information(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "information")
            log.info("updated info link")

    def _delete_link_elementset(self, link):
        onLine = link.getparent().getparent().getparent()
        p = onLine.getparent()
        p.remove(onLine)
        self.tree_changed = True

    def DELETE_link_no_protocol(self, uuid, link_to_delete):
        if link_to_delete != "":
            links = self._get_resources_no_protocol()
            for link in links:
                if link.findtext("gmd:linkage/gmd:URL", namespaces=self.namespaces) == link_to_delete:
                    self._delete_link_elementset(link)
                    log.info("deleted link with no protocol: {link}".format(link=link_to_delete))


    def DELETE_link(self,uuid, link_to_delete):
        if link_to_delete != "":
            links = self._current_link_url_elements()
            for link in links:
                if link.text == link_to_delete:
                    self._delete_link_elementset(link)
                    log.info("deleted link: {link}".format(link=link_to_delete))



    def NEW_topic_categories(self, uuid, new_topic_categories):
        """
        This is heinous. I'm sorry.
        """
        cat_list = new_topic_categories.split(self.INNER_DELIMITER)
        log.debug("NEW TOPIC INPUT: " + new_topic_categories)

        if len(cat_list) == 1 and cat_list[0] == "":
            return

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
            self.tree_changed = True

    def NEW_keywords_place(self, uuid, new_keywords):
        update = self._multiple_element_update(uuid, new_keywords, "keywords_place")
        log.info("updated place keywords")

    def NEW_keywords_theme(self, uuid, new_keywords):
        update = self._multiple_element_update(uuid, new_keywords, "keywords_theme")
        log.info("updated theme keywords")



    def _make_new_descriptive_keywords(self, tree):

        if len(dk) > 0:
            print("returning dk")
            dk[-1].addnext(etree.Element("{ns}descriptiveKeywords".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces))
            return dk[-1].getnext()
        else:
            md_di = tree.find(self.XPATHS[self.schema]["md_data_identification"], namespaces=self.namespaces)
            if md_di is not None:
                print("making new dk")
                return etree.SubElement(md_di, "{ns}descriptiveKeywords".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)


    def _make_new_keyword_thesaurus_elements(self):
        tree = self.record_etree
        dk = tree.findall(self.XPATHS[self.schema]["descriptive_keywords"], namespaces=self.namespaces)
        thesaurus = etree.parse("xml_snippets/thesaurus_gemet.xml")
        dk[-1].addnext(thesaurus.getroot())
        return dk[-1].getnext().find("gmd:MD_Keywords", namespaces=self.namespaces)


    def _make_new_keyword_anchor(self, value, uri, parent_node):
        element = etree.Element("{ns}keyword".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        child_element = etree.SubElement(element,"{ns}Anchor".format(ns="{"+self.namespaces["gmx"]+"}"))
        child_element.set("{"+ self.namespaces["xlink"]+"}href", self.GEMET_ANCHOR_BASE_URI + uri)
        child_element.text = value
        parent_node.insert(0, element)


    def _keywords_theme_gemet_update(self, uuid, new_values_string, new_ids_string):
        """
        This is heinous. I'm sorry.
        """
        log.debug("NEW VALUE INPUT: " + new_values_string)

        new_values_list = new_values_string.split(self.INNER_DELIMITER)
        new_ids_list = new_ids_string.split(self.INNER_DELIMITER)

        if len(new_values_list) == 1 and new_values_list[0] == "" or \
            len(new_ids_list) == 1 and new_ids_list[0] == "":
            return

        tree = self.record_etree
        tree_changed = False
        thesaurus_xpath = self.XPATHS[self.schema]["keywords_theme_gemet"]
        existing_thesaurus = tree.xpath(thesaurus_xpath, namespaces=self.namespaces_no_empty)


        if len(existing_thesaurus) == 0:
            md_kw = self._make_new_keyword_thesaurus_elements()
            tree_changed = True

            for index, value in enumerate(new_values_list):
                self._make_new_keyword_anchor(value, new_ids_list[index], md_kw)

        else:
            existing_values = existing_thesaurus[0].getparent().getparent().getparent().getparent().findall("gmd:keyword/gmx:Anchor", namespaces=self.namespaces)
            existing_values_parent = existing_values[0].getparent().getparent()
            existing_values_text = [i.text for i in existing_values]
            existing_ids = [i.get("{{ns}}href".format(ns="{"+self.namespaces["xlink"]+"}")) for i in existing_values]

            log.debug("EXISTING VALUES: " + ", ".join(existing_values_text))

            add_values = list(set(new_values_list) - set(existing_values_text))
            add_ids = list(set(new_ids_list) - set(existing_ids))
            delete_values = list(set(existing_values_text) - set(new_values_list))
            delete_ids = list(set(existing_ids) - set(new_ids_list))

            log.debug("VALUES TO ADD: " + ", ".join(add_values))
            log.debug("VALUES TO DELETE: " + ", ".join(delete_values))

            for delete_value in delete_values:
                #TODO abstract out keyword specifics
                del_ele = tree.xpath("//gmd:keyword/gmx:Anchor[text()='{val}']".format(val=delete_value), namespaces=self.namespaces_no_empty)
                if len(del_ele) == 1:
                    p = del_ele[0].getparent()
                    p.remove(del_ele[0])
                    pp = p.getparent()
                    pp.remove(p)
                    tree_changed = True

            for value in add_values:
                self._make_new_keyword_anchor(value, add_ids[index], existing_values_parent)
                tree_changed = True

        if tree_changed:
            self.tree_changed = True

    def NEW_keywords_theme_gemet_name(self, uuid, gemet_names):
        gemet_ids = self.row["NEW_keywords_theme_gemet_id"]
        self._keywords_theme_gemet_update(uuid, gemet_names, gemet_ids)
        log.info("updated gemet keywords")


    def NEW_keywords_place_geonames(self, uuid, geonames):
        log.debug("updated geonames keywords (but not really)")



    def _date_or_datetime(self,date_element):
        e = date_element.find("gco:Date", namespaces=self.namespaces)
        if e is None:
            e = date_element.find("gco:Datetime", namespaces=self.namespaces)
        return e

    def _add_gcodate_to_date(self, date_elem, new_date):
        date_e = etree.SubElement(date_elem,
            "{ns}Date".format(ns="{"+self.namespaces["gco"]+"}"),
            nsmap=self.namespaces)
        date_e.text = new_date

    def _add_datetypecode_to_datetype(self,datetype,date_type):
        datetypecode = etree.SubElement(datetype,
            "{ns}CI_DateTypeCode".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)
        datetypecode.set("codeList","http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode")
        datetypecode.set("codeListValue", date_type)
        datetypecode.set("codeSpace","002")
        datetypecode.text = date_type

    def _add_datetype_to_date(self, ci_date, date_type):
        datetype = etree.SubElement(ci_date,
            "{ns}dateType".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)
        self._add_datetypecode_to_datetype(datetype, date_type)


    def _create_date(self, date_elem, new_date, date_type):
        ci_date = etree.SubElement(date_elem,
            "{ns}CI_date".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)
        self._add_datetype_to_date(ci_date, date_type)

        date_elem2 = etree.SubElement(ci_date,
            "{ns}date".format(ns="{"+self.namespaces["gmd"]+"}"),
            nsmap=self.namespaces)
        self._add_gcodate_to_date(date_elem2, new_date)

    def _check_for_and_remove_nilreason(self, elem):
        nil_attrib = "{gco}nilReason".format(gco="{"+self.namespaces["gco"]+"}")
        nil = elem.get(nil_attrib)
        if nil is not None:
            elem.attrib.pop(nil_attrib)
        self.tree_changed = True

    def _update_date(self, uuid, new_date, date_type):
        #TODO get this xpath outta here
        xpath = "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode/@codeListValue='{datetype}']/gmd:date".format(datetype=date_type);
        new_date_parsed = parser.parse(new_date)
        iso_date = new_date_parsed.isoformat()[:10]
        tree = self.record_etree

        date_element = tree.xpath(xpath,
            namespaces=self.namespaces_no_empty)

        if len(date_element) >= 1:
            log.debug("Found date element with matching type in citation")
            self._check_for_and_remove_nilreason(date_element[0])
            date_type_elem = self._date_or_datetime(date_element[0])
            if date_type_elem is not None:
                if date_type_elem.tag.endswith("Date"):
                    date_type_elem.text = iso_date
                    self.tree_changed = True
                elif date_type_elem.tag.endswith("DateTime"):
                    date_type_elem.text = new_date_parsed.isoformat()+"Z"
                    self.tree_changed = True
            else:
                log.debug("Did not find date type in date element. Creating gco:Date.")
                self._add_gcodate_to_date(date_element[0], iso_date)
                self.tree_changed = True
        else:
            log.debug("No date element matching type found, looking for Citation.")
            # look for ancestor date element
            citation_xpath = self.XPATHS[self.schema]["citation"]
            citation_element = tree.xpath(citation_xpath,
                namespaces=self.namespaces_no_empty)

            if len(citation_element) >= 1:
                log.debug("Found Citation")
                date_elem = citation_element[0].find("gmd:date", namespaces=self.namespaces)
                if date_elem is not None:
                    log.debug("Found date in citation")
                    self._check_for_and_remove_nilreason(date_elem)
                    date_children = date_elem.getchildren()

                    if len(date_children) == 0:
                        self._create_date(date_elem, iso_date, date_type)
                        self.tree_changed = True
                    else:
                        ci_date = date_elem.find("gmd:CI_Date", namespaces=self.namespaces)

                        if ci_date is None:
                            log.debug("date is malformed.")
                            [date_elem.remove(i) for i in date_children]
                            self._create_date(date_elem, iso_date, date_type)
                            self.tree_changed = True
                        elif ci_date.find("gmd:dateType", namespaces=self.namespaces) is None:
                            self._add_datetype_to_date(ci_date, date_type)


                else:
                    date_elem = etree.SubElement(citation_element[0],
                        "{ns}date".format(ns="{"+self.namespaces["gmd"]+"}"),
                        nsmap=self.namespaces)
                    self._create_date(date_elem, iso_date, date_type)
                    self.tree_changed = True


    def _parse_snippet(self, snippet_name, path_to_snippet="xml_snippets"):
        snippet_tree = etree.parse(os.path.join(path_to_snippet, snippet_name))
        snippet_root = snippet_tree.getroot()
        return snippet_root

    def _add_extent(self):
        existing_extent = self.record_etree.xpath(self.XPATHS[self.schema]["extent"], namespaces=self.namespaces_no_empty)
        last_extent = existing_extent[-1]
        new_extent = etree.Element("{ns}extent".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        ex_extent = etree.SubElement(new_extent, "{ns}EX_Extent".format(ns="{"+self.namespaces["gmd"]+"}"), nsmap=self.namespaces)
        last_extent.addnext(new_extent)
        log.debug("Added new extent.")
        return ex_extent


    def _add_temporal_extent(self, new_date, ex_extent, temporal_extent_type):
        if temporal_extent_type == "start" or temporal_extent_type == "end":
            temporal_extent_base = self._parse_snippet("extent_temporal_range.xml")
        else:
            temporal_extent_base = self._parse_snippet("extent_temporal_instant.xml")
        ex_extent.append(temporal_extent_base)
        log.debug("added temporal extent. recursing to set value")
        self.tree_changed = True
        self._update_temporal_extent(new_date, temporal_extent_type)


    def _update_temporal_extent(self, new_date, temporal_extent_type):

        if new_date != "now":
            new_date_parsed = parser.parse(new_date)
            iso_date = new_date_parsed.isoformat()[:10]
        else:
            iso_date = "now"

        tree = self.record_etree

        xpath = self.XPATHS[self.schema]["temporalextent"]
        temporalextent = tree.xpath(xpath,
            namespaces=self.namespaces_no_empty)

        if len(temporalextent) >= 1:
            te = temporalextent[0]
            xpath = self.XPATHS[self.schema]["temporalextent_" + temporal_extent_type]
            log.debug(xpath)
            #import pdb; pdb.set_trace()
            te_elem = te.find(xpath, namespaces=self.namespaces)
            if te_elem is not None:
                te_elem.text = iso_date
                log.debug("found and updated temporal extent: {iso}".format(iso=iso_date))
                self.tree_changed = True
            else:
                p_ex_extent = te.getparent()
                p_ex_extent.remove(te)
                log.debug("adding temporal extent to existing extent")
                self._add_temporal_extent(new_date, p_ex_extent, temporal_extent_type)
        else:
            ex_extent = self._add_extent()
            self._add_temporal_extent(new_date, ex_extent, temporal_extent_type)


    def NEW_date_publication(self, uuid, new_date):
        if new_date != "":
            update = self._update_date(uuid, new_date, "publication")
            log.info("updated publication date!")


    def NEW_date_revision(self, uuid, new_date):
        if new_date != "":
            update = self._update_date(uuid, new_date, "revision")
            log.info("updated revision date!")




    def NEW_temporal_start(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "start")
            log.info("updated temporal start date!")

    def NEW_temporal_end(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "end")
            log.info("updated temporal end date!")

    def NEW_temporal_instant(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "instant")
            log.info("updated temporal instant date!")


    def NEW_contact_organization(self):
        #stub
        pass


    def NEW_contact_individual(self):
        #stub
        pass


    def update_timestamp(self, uuid):
        ts = datetime.now().isoformat()
        val = ts
        tree = self.record_etree
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


    def _get_etree_for_record(self, uuid):
        """
        Set self.record_etree to etree ElementTree of record with inputted uuid.
        """

        xml = self.records[uuid].xml
        root = etree.fromstring(xml)
        return etree.ElementTree(root)


    def get_record_by_id(self, uuid):
        """
        Requests a record via the provided uuid. Sets self.records[uuid] to the result. Returns nothing.
        """

        if self.schema == "iso19139":
            outschema = "http://www.isotc211.org/2005/gmd"
            log.debug("get_record_by_id: requesting fresh XML.")
            self.csw.getrecordbyid(id=[str(uuid)], outputschema=outschema)
            time.sleep(1)
            if self.csw.records.has_key(uuid):
                log.debug("get_record_by_id: got the xml")
                self.records[uuid] = self.csw.records[uuid]

        else:
            # unused Dublin Core hack for dc:identifiers that are URIs
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
        """
        Iterates through the inputted CSV, detects NEW_ fields, and executes
        update functions as needed.
        """

        for row in self.reader:
            log.debug(row)
            self.row_changed = False
            self.tree_changed = False
            self.record_etree = False

            self.row = row
            if row.has_key("uuid"):
                self.uuid = row["uuid"]
            elif row.has_key("UUID"):
                self.uuid = row["UUID"]
            else:
                sys.exit("No uuid column found. Must be named uuid or UUID.")

            if self.uuid == "DELETED" or self.uuid == "SKIP":
                continue

            log.debug(self.uuid)

            if row.has_key("schema"):
                self.schema = row["schema"]
            else:
                log.info("No 'schema' column. Defaulting to iso19139.")
                self.schema = "iso19139"

            self.get_record_by_id(self.uuid)
            self.record_etree = self._get_etree_for_record(self.uuid)

            for field in self.fieldnames:
                if field.lower() == "uuid":
                    continue

                if field in self.field_handlers[self.schema] and row.has_key(field):
                    self.field_handlers[self.schema][field].__call__(self.uuid, row[field])

            if self.records.has_key(self.uuid) and self.tree_changed:
                log.debug("replacing entire XML")
                new_xml = etree.tostring(self.record_etree)
                self.row_changed = True
                t = self.csw.transaction(ttype="update",
                    typename='csw:Record',
                    record=new_xml,
                    identifier=self.uuid)
                #log.debug(self.csw.request)
                #log.debug(self.csw.response)
                time.sleep(2)
                log.info("Updated: {uuid}\n\n\n".format(uuid=self.uuid))
            else:
                log.info("No change: {uuid}\n\n\n".format(uuid=self.uuid))


def main():
    parser = argparse.ArgumentParser( formatter_class=argparse.RawDescriptionHelpFormatter,
        description=open("README.md","rU").read())
    parser.add_argument("input_csv",help="indicate path to the csv containing the updates")
    args = parser.parse_args()
    f = UpdateCSW(CSW_URL, USER, PASSWORD, args.input_csv)
    f.process_spreadsheet()

if __name__ == "__main__":
    sys.exit(main())
