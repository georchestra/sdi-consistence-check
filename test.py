#!/usr/bin/python3
from getpass import getpass

from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.fes import PropertyIsEqualTo, And

from credentials import Credentials
from cswquerier import CSWQuerier
from inconsistency import LayerNotFoundInconsistency
from owscheck import CachedOwsServices

my_username = None
my_password = None
gnurl = "http://www.geopicardie.fr/geonetwork/srv/fre/csw"

csw_q = CSWQuerier(gnurl, username=my_username, password=my_password)

creds = Credentials()
creds.addFromUrl(gnurl, my_username, my_password)
geoserver_services = CachedOwsServices(creds)

csw = CatalogueServiceWeb(gnurl, username=my_username, password=my_password)
filter = [PropertyIsEqualTo("Type", "service")]
csw.getrecords2(constraints=filter,
                esn='full',
                outputschema=namespaces['gmd'],
                startposition=0,
                maxrecords=1000000)

for uuid in csw.records:
    if uuid != "0e1785b285ccecc853c52bdb4354b6a00df84cbe":
        continue
    md = csw.records[uuid]
    for operateson in md.serviceidentification.operateson:
        print ("Operates on %s" % operateson["href"])


# errors = []
#
# while True:
#     print("---------------------------------------------------------------------------------------------")
#
#     res = csw_q.get_records()
#
#     # no more results, we should stop
#     if csw_q.csw.results['returned'] == 0:
#         break
#
#     for uuid in res:
#         print("\nUUID : %s" % uuid)
#
# #        for uri in csw.records[uuid].uris:
#         for uri in csw_q.get_md(uuid).uris:
#             # print("%s %s %s" % (uri['protocol'], uri['url'], uri['name']))
#             try:
#                 if uri["protocol"] == "OGC:WMS":
#                     geoserver_services.checkWmsLayer(uri["url"], uri["name"])
#                     print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
#                 elif uri["protocol"] == "OGC:WFS":
#                     geoserver_services.checkWfsLayer(uri["url"], uri["name"])
#                     print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
#                 else:
#                     print("\tSkipping URI : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
#             except LayerNotFoundInconsistency as ex:
#                 ex.set_md_uuid(uuid)
#                 errors.append(ex)
#                 print("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s" % (uuid, uri['protocol'], uri['url'], uri['name'], ex))
#
#
# for error in errors:
#     print(error)