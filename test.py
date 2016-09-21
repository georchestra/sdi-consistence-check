#!/usr/bin/python3

from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, And, Not, Or
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService


def generate_filter(excluded_uuid):
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    non_havested = PropertyIsEqualTo("_isHarvested", "n")
    if len(excluded_uuid) == 0:
        filters = [is_dataset, non_havested]
    elif len(excluded_uuid) == 1:
        filters = [is_dataset, non_havested, Not([PropertyIsEqualTo("fileIdentifier", excluded_uuid[0])])]
    else:
        filters = [is_dataset, non_havested, Not(Or([PropertyIsEqualTo("fileIdentifier", uuid) for uuid in excluded_uuid]))]
    return [And(filters)]


class CSWQuerier:

    max_records = 100
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    non_havested = PropertyIsEqualTo("_isHarvested", "n")

    def __init__(self, url):
        self.csw = CatalogueServiceWeb(url)
        self.mds_not_parsable = []
        self.reset()

    def reset(self):
        self.start = 00
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
                print("truite")
                return ValueError("Unable to find bogus MD")

    def generate_filter(self):
        if len(self.mds_not_parsable) == 0:
            filters = [self.is_dataset, self.non_havested]
        elif len(self.mds_not_parsable) == 1:
            filters = [self.is_dataset,
                       self.non_havested,
                       Not([PropertyIsEqualTo("truite", self.mds_not_parsable[0])])]
        else:
            filters = [self.is_dataset,
                       self.non_havested,
                       Not(Or([PropertyIsEqualTo("fileIdentifier", uuid) for uuid in self.mds_not_parsable]))]
        return [And(filters)]



csw_q = CSWQuerier('http://127.0.0.1:8080/geonetwork/srv/eng/csw')
csw_q.mds_not_parsable.append("FR-120066022-MDLOT-3761")
csw_q.get_records()
exit(0)

class WMSService:
    """
    Handle cache of WebMapService() object, avoid useless call to getCapabilities
    """

    def __init__(self, url):
        self.wms = WebMapService(url)

    def has_layer(self, name):
        try:
            self.wms[name]
            return True
        except KeyError:
            return False


def test_GS_WFS_Layer(uri, name):
    # print("Searching for WFS layer %s at %s ... " % (name, uri), end="")
    wfs = WebFeatureService(uri)
    # print(list(wfs.contents))
    wfs[name].title
    # print(wfs[name].title)


def search_for_error(csw, index):
    start = index
    next_record = -1
    while start != next_record:
            try:
                csw.getrecords2(constraints=generate_filter([]), esn='full', startposition=start, maxrecords=1)
                start = csw.results['nextrecord']
                print("Start : %s " % start)
            except ValueError:
                csw.getrecords2(constraints=generate_filter([]),  startposition=start, maxrecords=1)
                for uuid in csw.records:
                    print("---------------------------------------------------> Error on %s at %s" % (uuid, start))
                    return (uuid, start)


# csw = CatalogueServiceWeb('https://sdi.georchestra.org/geonetwork/srv/eng/csw')
#csw = CatalogueServiceWeb('https://www.pigma.org/geonetwork/srv/eng/csw')
#csw_q = CSWQuerier('https://www.pigma.org/geonetwork/srv/eng/csw')
csw_q = CSWQuerier('http://localhost:8080/geonetwork/srv/eng/csw')
wmss = {}

is_dataset = PropertyIsEqualTo("Type", "dataset")
is_service = PropertyIsEqualTo("Type", "service")
non_havested = PropertyIsEqualTo("_isHarvested", "n")

csw_q.mds_not_parsable.append("FR-120066022-MDLOT-3761")
csw_q.get_records()

# not_bogus = Not([PropertyIsEqualTo("fileIdentifier", "FR-120066022-MDLOT-3761")])
#
# csw.getrecords2(constraints=[And([is_dataset, is_service, not_bogus])], esn='full', startposition=1400, maxrecords=100)
# exit(0)

# csw.getrecords2(constraints=[is_dataset, non_havested], esn='full', startposition=1419, maxrecords=1)
# for uuid in csw.records:
#     print("Error on %s" % uuid)
# exit(0)

md_not_parsable = []

start=1300
next_record = -1
while True: # start != next_record:
    print("---------------------------------------------------------------------------------------------")
    # try:
    #     csw.getrecords2(constraints=generate_filter(md_not_parsable), esn='full', startposition=start, maxrecords=100)
    #     print(csw.results)
    #     start = csw.results['nextrecord']
    #
    # except ValueError:
    #     (uuid_error, index) = search_for_error(csw, start)
    #     print("Error on MD : %s (index:%s)" % (uuid_error, index))
    #     md_not_parsable.append(uuid_error)
    #     continue
    res = csw_q.get_records()

#    for uuid in csw.records:
    for uuid in res:
        print("\nUUID : %s" % uuid)

#        for uri in csw.records[uuid].uris:
        for uri in csw_q.get_md(uuid).uris:
            #print("%s %s %s" % (uri['protocol'], uri['url'], uri['name']))
            try:
                if uri["protocol"] == "OGC:WMS":

                    # Add WMS server/workspace if not already present
                    if uri["url"] not in wmss.keys():
                        wmss[uri["url"]] = WMSService(uri["url"])

                    if wmss[uri["url"]].has_layer(uri["name"]):
                        print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
                    else:
                        print("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s" % (uuid, uri['protocol'], uri['url'], uri['name']))

                elif uri["protocol"] == "OGC:WFS":
                    test_GS_WFS_Layer(uri["url"], uri["name"])
                else:
                    print("\tSkipping URI : %s" % uri["protocol"])
            except KeyError:
                print("Cannot find Layer ON GS : %s %s %s %s" % (uuid, uri['protocol'], uri['url'], uri['name'] ) )









# __dict__.keys()


#  constraints=[],
# sortby=None,
# typenames='csw:Record',
# esn='summary',
# outputschema=namespaces['csw'],
# format=outputformat,
# startposition=0,
# maxrecords=10,
# cql=None,
# xml=None,
# resulttype='results'):
#         """