from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.fes import PropertyIsEqualTo, Not, Or, And
import re
from contextlib import suppress
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from credentials import Credentials
from inconsistency import Inconsistency
from owscheck import CachedOwsServices


class CSWQuerier:

    max_records = 100
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    is_service = [PropertyIsEqualTo("Type", "service")]
    non_havested = PropertyIsEqualTo("_isHarvested", "n")

    protocol_regexp = re.compile("^OGC:(?P<type>WMS|WFS)(?:-(?P<version>\d+(?:\.\d+)*)(?:-[\w-]+)?)?$", re.IGNORECASE)

    def __init__(self, url, credentials=Credentials(), cached_ows_services=None):
        (username, password) = credentials.getFromUrl(url)
        if cached_ows_services is None:
            self.owsServices = CachedOwsServices(credentials=credentials)
        else:
            self.owsServices = cached_ows_services
        self.csw = CatalogueServiceWeb(url, username=username, password=password)
        self.mds_not_parsable = []
        self.reset()

    def reset(self):
        self.start = 0
        self.md_count = -1

    def get_records(self):
        try:
            self.csw.getrecords2(constraints=self.generate_filter(),
                                 esn='full',
                                 startposition=self.start,
                                 maxrecords=self.max_records)
            print("CSWQuerier.get_records() results : %s (start=%s, max=%s)" % (self.csw.results, self.start, self.max_records))
            self.start += self.csw.results['returned']
        except ValueError:
            self.search_for_error()
            return self.get_records()

        return self.csw.records

    def get_md(self, uuid):
        return self.csw.records[uuid]

    def search_for_error(self):
        index = self.start
        while index < self.start + self.max_records:
            try:
                self.csw.getrecords2(constraints=self.generate_filter(), esn='full', startposition=index, maxrecords=1)
                print("Index : %s" % index)
                index += 1
            except ValueError:
                self.csw.getrecords2(constraints=self.generate_filter(), startposition=index, maxrecords=1)
                for uuid in self.csw.records:
                    self.mds_not_parsable.append(uuid)
                    print("-----------------------------------------------------------------------------------------------------------------------------------> Error on %s at %s" % (uuid, index))
                    return

                return ValueError("Unable to find bogus MD")

    def generate_filter(self):
        if len(self.mds_not_parsable) == 0:
            filters = [self.is_dataset, self.non_havested]
            return [self.is_dataset]
        elif len(self.mds_not_parsable) == 1:
            filters = [self.is_dataset,
                       # self.non_havested,
                       Not([PropertyIsEqualTo("truite", self.mds_not_parsable[0])])]
        else:
            filters = [self.is_dataset,
                       # self.non_havested,
                       Not(Or([PropertyIsEqualTo("fileIdentifier", uuid) for uuid in self.mds_not_parsable]))]
        return [And(filters)]

    def get_service_mds(self):

        self.csw.getrecords2(constraints=self.is_service,
                             esn='full',
                             outputschema=namespaces['gmd'],
                             startposition=0,
                             maxrecords=1000000)

        return self.csw.records

    def check_service_md(self, uuid, geoserver_to_check=[]):

        md = self.csw.records[uuid]

        # check if this is an interesting service md (contains "coupledResource" or "operatesOn" tag)
        if len(md.serviceidentification.operateson) == 0:
            # raise error ?
            return

        print("\nService metadata : UUID=%s" % uuid)

        # retrieve geoserver base URL (getCapabilities)
        url = None
        for op in md.serviceidentification.operations:
            if op['name'] == "GetCapabilities":
                url = op['connectpoint'][0].url
                protocol = op['connectpoint'][0].protocol

        if url is None:
            print("\tSkipping : no GetCapabilities URL found")
            # raise error ?
            return

        url_parsed = urlparse(url)
        if url_parsed.hostname not in geoserver_to_check:
            print("\tSkipping : geoserver : %s not in authorized list (%s)" % (url_parsed.hostname, url))
            # raise error ?
            return

        # try to read protocol
        matches = self.protocol_regexp.match(protocol)
        if matches is None:
            print("Invalid protocol : %s " % protocol)
            # raise error ?
            return

        type = matches.group("type")
        version = matches.group("version")
        print("Server Type: %s Version: %s URL: %s" % (type, version, url))

        root = ET.fromstring(md.xml.decode())
        xpath = ".//{http://www.isotc211.org/2005/srv}coupledResource"
        # xpath = ".//{http://www.isotc211.org/2005/srv}operatesOn"
        res = root.findall(xpath)
        for r in res:
            operationName = identifier = layer_name = None
            with suppress(AttributeError):
                operationName = r.find(".//{http://www.isotc211.org/2005/srv}operationName/{http://www.isotc211.org/2005/gco}CharacterString").text
                identifier = r.find(".//{http://www.isotc211.org/2005/srv}identifier/{http://www.isotc211.org/2005/gco}CharacterString").text
                layer_name = r.find(".//{http://www.isotc211.org/2005/gco}ScopedName").text
            if identifier is not None and layer_name is not None:
                print("\tcoupledRessources:")
                print("\tOperation : %s" % operationName)
                print("\tidentifier : %s" % identifier)
                print("\tLayer Name: %s" % layer_name)
                print("")

                try:
                    if type.lower() == "wms":
                        self.owsServices.checkWmsLayer(url, layer_name)
                    elif type.lower() == "wfs":
                        self.owsServices.checkWfsLayer(url, layer_name)
                    else:
                        raise Inconsistency("Invalid service type : %s" % type)
                    print("Check OK : UUID: %s Layer Name: %s on %s" % (uuid, layer_name, url))
                except Inconsistency as e:
                    e.md_uuid = uuid
                    print("Adding inconsistency : %s" % str(e))
                    raise e
