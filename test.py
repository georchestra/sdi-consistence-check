#!/usr/bin/python3
from getpass import getpass

from credentials import Credentials
from cswquerier import CSWQuerier
from inconsistency import LayerNotFoundInconsistency
from owscheck import CachedOwsServices

my_username = "xxxx"
my_password = getpass()
gnurl = 'https://sdi.georchestra.org/geonetwork/srv/eng/csw'

csw_q = CSWQuerier(gnurl, username=my_username, password=my_password)

creds = Credentials()
creds.addFromUrl(gnurl, my_username, my_password)
geoserver_services = CachedOwsServices(creds)

errors = []

while True:
    print("---------------------------------------------------------------------------------------------")

    res = csw_q.get_records()

    # no more results, we should stop
    if csw_q.csw.results['returned'] == 0:
        break

    for uuid in res:
        print("\nUUID : %s" % uuid)

#        for uri in csw.records[uuid].uris:
        for uri in csw_q.get_md(uuid).uris:
            # print("%s %s %s" % (uri['protocol'], uri['url'], uri['name']))
            try:
                if uri["protocol"] == "OGC:WMS":
                    geoserver_services.checkWmsLayer(uri["url"], uri["name"])
                    print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
                elif uri["protocol"] == "OGC:WFS":
                    geoserver_services.checkWfsLayer(uri["url"], uri["name"])
                    print("\tURI OK : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
                else:
                    print("\tSkipping URI : %s %s %s" % (uri["protocol"], uri['url'], uri['name']))
            except LayerNotFoundInconsistency as ex:
                ex.set_md_uuid(uuid)
                errors.append(ex)
                print("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s" % (uuid, uri['protocol'], uri['url'], uri['name'], ex))


for error in errors:
    print(error)