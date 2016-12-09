import logging
import re
import warnings
import xml.etree.ElementTree as ET
from contextlib import suppress
from urllib.parse import urlparse

from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.fes import PropertyIsEqualTo, Not, Or, And

from credentials import Credentials
from inconsistency import Inconsistency
from owscheck import CachedOwsServices


class CSWQuerier:
    max_records = 100
    is_dataset = [PropertyIsEqualTo("Type", "dataset")]
    is_service = [PropertyIsEqualTo("Type", "service")]
    non_havested = PropertyIsEqualTo("_isHarvested", "n")

    protocol_regexp = re.compile("^OGC:(?P<type>WMS|WFS)(?:-(?P<version>\d+(?:\.\d+)*)(?:-[\w-]+)?)?$", re.IGNORECASE)

    def __init__(self, url, credentials=Credentials(), cached_ows_services=None, logger=None):
        (username, password) = credentials.getFromUrl(url)
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger("cswquerier")
            self.logger.addHandler(logging.nullHandler())
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
        self.csw.getrecords2(constraints=self.generate_filter(),
                             esn='full',
                             startposition=self.start,
                             maxrecords=self.max_records)
        self.logger.debug("CSWQuerier.get_records() results : %s (start=%s, max=%s)",
                          self.csw.results, self.start, self.max_records)
        self.start += self.csw.results['returned']
        return self.csw.records

    def get_md(self, uuid):
        return self.csw.records[uuid]

    # TODO: dead code ?
    def generate_filter(self):
        if len(self.mds_not_parsable) == 0:
            filters = [self.is_dataset, self.non_havested]
            return self.is_dataset
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
        # do not take care of FutureWarnings issued by OWSLib
        with warnings.catch_warnings():
            self.csw.getrecords2(constraints=self.is_service,
                                 esn='full',
                                 outputschema=namespaces['gmd'],
                                 startposition=0,
                                 maxrecords=1000000)

            return self.csw.records


    def get_data_mds(self):
        self.csw.getrecords2(constraints=self.is_dataset,
                             esn='full',
                             outputschema=namespaces['gmd'],
                             startposition=0,
                             maxrecords=1000000)
        return self.csw.records

    def get_all_records(self, constraint):
        startpos = 0
        mds = {}
        while True:
            self.csw.getrecords2(constraint,
                                 esn='full',
                                 outputschema=namespaces['gmd'],
                                 startposition=startpos,
                                 maxrecords=self.max_records)
            for uuid in self.csw.records:
                mds[uuid] = self.csw.records[uuid]
            startpos = len(mds) + 1
            # end condition
            if self.csw.results['nextrecord'] == 0:
                break
        return mds


    def check_service_md(self, mds, mdd, geoserver_to_check=[]):
        warnings.simplefilter("ignore")

        # check if this is an interesting service md (contains "coupledResource" or "operatesOn" tag)
        if len(mds.serviceidentification.operateson) == 0:
            # raise error ?
            return

        self.logger.info("\nData metadata: uuid %s \"%s\"", mdd.identifier, mdd.identification.title)
        self.logger.info("Service metadata: uuid %s \"%s\"", mds.identifier, mds.identification.title)

        # retrieve geoserver base URL (getCapabilities)
        url = None
        for op in mds.serviceidentification.operations:
            if op['name'] == "GetCapabilities":
                url = op['connectpoint'][0].url
                protocol = op['connectpoint'][0].protocol

        if url is None:
            self.logger.debug("\tSkipping : no GetCapabilities URL found")
            # raise error ?
            return

        url_parsed = urlparse(url)
        if url_parsed.hostname not in geoserver_to_check:
            self.logger.debug("\tSkipping : geoserver : %s not in authorized list (%s)" % (url_parsed.hostname, url))
            # raise error ?
            return

        # try to read protocol
        matches = self.protocol_regexp.match(protocol)
        if matches is None:
            self.logger.debug("Invalid protocol : %s " % protocol)
            # raise error ?
            return

        type = matches.group("type")
        version = matches.group("version")
        self.logger.debug("Server Type: %s Version: %s URL: %s" % (type, version, url))

        root = ET.fromstring(md.xml.decode())
        xpath = ".//{http://www.isotc211.org/2005/srv}coupledResource"
        # xpath = ".//{http://www.isotc211.org/2005/srv}operatesOn"
        res = root.findall(xpath)
        for r in res:
            operationName = identifier = layer_name = None
            with suppress(AttributeError):
                operationName = r.find(
                    ".//{http://www.isotc211.org/2005/srv}operationName/{http://www.isotc211.org/2005/gco}CharacterString").text
                identifier = r.find(
                    ".//{http://www.isotc211.org/2005/srv}identifier/{http://www.isotc211.org/2005/gco}CharacterString").text
                layer_name = r.find(".//{http://www.isotc211.org/2005/gco}ScopedName").text
            if identifier is not None and layer_name is not None:
                self.logger.debug("\tcoupledRessources:")
                self.logger.debug("\tOperation : %s" % operationName)
                self.logger.debug("\tidentifier : %s" % identifier)
                self.logger.debug("\tLayer Name: %s" % layer_name)
                self.logger.debug("")

                try:
                    if type.lower() == "wms":
                        self.owsServices.checkWmsLayer(url, layer_name)
                    elif type.lower() == "wfs":
                        self.owsServices.checkWfsLayer(url, layer_name)
                    else:
                        raise Inconsistency("Invalid service type : %s" % type)
                        self.logger.debug("Check OK : UUID: %s Layer Name: %s on %s" % (uuid, layer_name, url))
                except Inconsistency as e:
                    e.md_uuid = uuid
                    self.logger.debug("Adding inconsistency : %s" % str(e))
                    raise e
