from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
import logging
import sys
import json
from datetime import datetime
import unicodecsv as csv
import pdb


# for example http://45.55.225.162/geonetwork/srv/eng/csw-publication
CSW_URL = "http://45.55.225.162/geonetwork/srv/eng/csw-publication"

#logging stuff
log = logging.getLogger('owslib')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(log_formatter)
log.addHandler(ch)



class UpdateCSW(object):
    def __init__(self, url, username, password, input_csv_filename):
        self.csw = csw.CatalogueServiceWeb(url, username=username, password=password)
        self.records = {}
        self.reader = csv.DictReader(open(input_csv_filename,"rU"))
        self.fieldnames = self.reader.fieldnames
        self.fieldnames.remove("uuid")
        self.namespaces = self.get_namespaces()

        # these are the column names that will trigger a change
        self.field_handlers = {
            "NEW_title": self.NEW_title,
            "NEW_link_download": self.NEW_link_download,
            "NEW_link_service_wms": self.NEW_link_service_wms,
            "NEW_link_service_esri": self.NEW_link_service_esri,
            "NEW_link_information": self.NEW_link_information,
            "NEW_contact_organization": self.NEW_contact_organization,
            "NEW_contact_individual": self.NEW_contact_individual
        }
        self.XPATHS = {
            "NEW_title": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString",
            "date_creation": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/[@codeListValue='creation']",
            "contact_org": "gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString",
            "timestamp": "gmd:dateStamp/gco:DateTime",
            "online_resources": "*//gmd:CI_OnlineResource",
            "distribution_link": "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions[{index}]/gmd:MD_DigitalTransferOptions[1]/gmd:onLine[1]/gmd:CI_OnlineResource[1]/gmd:linkage[1]/gmd:URL[1]",
            "distributor_distribution_link": "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor[{distributor_index}]/gmd:MD_Distributor/gmd:distributorTransferOptions[{index}]/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL"
        }
        self.protocol_map = {
            "download": ["WWW:DOWNLOAD-1.0-http--download","download", "WWW:DOWNLOAD"],
            "information": ["WWW:LINK-1.0-http--link", "WWW:LINK"],
            "esri_service": ["ESRI:ArcGIS"],
            "wms_service": ["OGC:WMS"],
            "wfs_service": ["OGC:WFS"],
            "wcs_service": ["OGC:WCS"]
        }

    @staticmethod
    def get_namespaces():
        n = Namespaces()
        ns = n.get_namespaces(["gco","gmd","gml","gml32","gmx","gts","srv","xlink"])
        ns[None] = n.get_namespace("gmd")
        return ns


    @staticmethod
    def _filter_link_updates(field):
        if field.startswith("NEW_link"):
            return True


    def _get_links_from_record(self, uuid):
        """
        Generates two lists, one of transferOptions, and one of distributorTransferOptions.
        Maybe should combine the two, but maybe not since we're talking
        """

        self._get_etree_for_record(uuid)

        self.record_online_resources = self.record_etree.findall(self.XPATHS["online_resources"], self.namespaces)

        # self.distribution_transferoptions = self.records[uuid].distribution.online


        # #more complicated, as we have to consider multiple possible distributors
        # # list per distributor in container list (meh)
        # self.distribution_distributortransferoptions = []
        # for distributor_index, distributor in enumerate(self.records[uuid].distribution.distributor):
        #     self.distribution_distributortransferoptions.append([])
        #     for i in distributor.online:
        #         self.distribution_distributortransferoptions[distributor_index].append(i)

    def _get_etree_for_record(self, uuid):
        xml = self.records[uuid].xml
        root = etree.fromstring(xml)
        self.record_etree = etree.ElementTree(root)


    def _check_for_links_to_update(self, link_type):
        protocols_list = self.protocol_map[link_type]
        links_to_update = filter(lambda resource,
            ns=self.namespaces,
            protocols=protocols_list: resource.findtext("gmd:protocol/gco:CharacterString", namespaces=ns) in protocols_list,
            self.record_online_resources
            )

        return links_to_update

    def NEW_title(self, uuid, new_title):
        t = self.csw.transaction(ttype="update",
            typename='csw:Record',
            propertyname=self.XPATHS["NEW_title"],
            propertyvalue=new_title,
            identifier=uuid)


    def update_links(self, uuid, new_link, link_type):
        links_to_update = self._check_for_links_to_update(link_type)
        for i in links_to_update:
            elem = i.find("gmd:linkage/gmd:URL", namespaces=self.namespaces)
            current_val = elem.text

            if current_val and current_val == new_link:
                print("Value is already set to {link}. Skipping!".format(link=new_link))
                continue

            xpath = self.record_etree.getpath(elem)
            xpath = "/".join(xpath.split("/")[2:])
            self.csw.transaction(ttype="update",
                typename='csw:Record',
                propertyname=xpath,
                propertyvalue=new_link,
                identifier=uuid)



    def NEW_link_download(self, uuid, new_link):
        self.update_links(links, uuid, new_link, "download")


    def NEW_link_service_esri(self, uuid, new_link):
        self.update_links(links, uuid, new_link, "esri_service")

    def NEW_link_service_wms(self, uuid, new_link):
        self.update_links(links, uuid, new_link, "wms_service")


    def NEW_link_information(self, uuid, new_link):
        self.update_links(links, uuid, new_link, "information")

    def NEW_contact_organization(self):
        #stub
        pass

    def NEW_contact_individual(self):
        #stub
        pass

    def update_timestamp(self, uuid):
        ts = datetime.now().isoformat()
        return self.csw.transaction(ttype="update",
            typename='csw:Record',
            propertyname=self.XPATHS["timestamp"],
            propertyvalue=ts,
            identifier=uuid)

    def get_record_by_id(self, uuid):
        self.csw.getrecordbyid(id=[uuid], outputschema="http://www.isotc211.org/2005/gmd")
        self.records[uuid] = self.csw.records[uuid]

    def process_spreadsheet(self):
        for row in self.reader:

            row_changed = False
            self.uuid = row["uuid"]

            if len(filter(self._filter_link_updates, self.fieldnames)) > 0:
                self.get_record_by_id(self.uuid)
                self._get_links_from_record(self.uuid)

            for field in self.fieldnames:

                if field in self.field_handlers:
                    row_changed=True
                    self.field_handlers[field].__call__(self.uuid, row[field])

            # TODO need to make change detection much more sophisticated (right now it updates timestamp even if it doesn't actually alter values)
            # also it adds datetime as timestamp, even if there's just a date, resulting to invalidating the record bc of two elements w/in timestamp
            #if row_changed:
            #    self.update_timestamp(self.uuid)


f = UpdateCSW(CSW_URL, raw_input("Please enter username: "), raw_input("Please enter password: "), "CSWUpdateTest3.csv")
f.process_spreadsheet()
