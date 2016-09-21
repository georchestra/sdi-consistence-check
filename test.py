#!/usr/bin/python3

from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, And, Not, Or
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService
from getpass import getpass
from requests.exceptions import HTTPError
from owslib.util import ServiceException

from inconsistency import LayerNotFoundInconsistency

my_username = "xxxx"
my_password = getpass()

class CSWQuerier:

    max_records = 100
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    non_havested = PropertyIsEqualTo("_isHarvested", "n")

    def __init__(self, url, username=None, password=None):
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




class GeoserverServices:
    """
    Handle cache of WebMapService() or WebFeatureService() objects, avoid useless call to getCapabilities
    """

    service_constructor = None

    def __init__(self, username, password):
        self.servers = {}
        self.username = username
        self.password = password

    def check_layer(self, url, name):
        if url not in self.servers.keys():
            try:
                self.servers[url] = self.__class__.service_constructor(url=url, username=self.username, password=self.password)
            except HTTPError as ex:
                raise LayerNotFoundInconsistency(layer_name=name, layer_url=url, md_uuid=None, msg="HTTPError: %s" % str(ex))
            except ServiceException as ex:
                raise LayerNotFoundInconsistency(layer_name=name, layer_url=url, md_uuid=None, msg="ServiceException: %s" % str(ex))
        try:
            self.servers[url][name]
        except KeyError:
            raise LayerNotFoundInconsistency(layer_name=name, layer_url=url, md_uuid=None, msg="Layer not found on GS")


class WFSServices(GeoserverServices):
    """
    Handle cache of WebFeatureService() object, avoid useless call to getCapabilities
    """
    service_constructor = WebFeatureService


class WMSServices(GeoserverServices):
    """
    Handle cache of WebMapService() object, avoid useless call to getCapabilities
    """
    service_constructor = WebMapService

csw_q = CSWQuerier('https://portail.sig.rennesmetropole.fr/geonetwork/srv/eng/csw', username=my_username, password=my_password)
wms_services = WMSServices(username=my_username, password=my_password)
wfs_services = WFSServices(username=my_username, password=my_password)

errors = []

while True:
    print("---------------------------------------------------------------------------------------------")

    res = csw_q.get_records()
    for uuid in res:
        print("\nUUID : %s" % uuid)

#        for uri in csw.records[uuid].uris:
        for uri in csw_q.get_md(uuid).uris:
            # print("%s %s %s" % (uri['protocol'], uri['url'], uri['name']))
            try:
                if uri["protocol"] == "OGC:WMS":
                    wms_services.check_layer(uri["url"], uri["name"])
                    print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
                elif uri["protocol"] == "OGC:WFS":
                    wfs_services.check_layer(uri["url"], uri["name"])
                    print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
                else:
                    print("\tSkipping URI : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
            except LayerNotFoundInconsistency as ex:
                ex.set_md_uuid(uuid)
                errors.append(ex)
                print("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s" % (uuid, uri['protocol'], uri['url'], uri['name'], ex))
