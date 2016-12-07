#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from time import gmtime, strftime

from owslib.util import ServiceException

from credentials import Credentials
from owscheck import OwsChecker
from inconsistency import Inconsistency, LayerNotFoundInconsistency
from cswquerier import CachedOwsServices, CSWQuerier

# Logging configuration
logger = logging.getLogger("owschecker")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)

creds = Credentials()

def print_error(errors):
    for error in errors:
        print("Error : %s" % str(error))

def load_credentials():
    """
    Loads the credentials file, which consists of a text file
    formatted with "hostname username password" and whose default location is set to
    ~/.sdichecker.

    :return: None.
    """
    try:
        with open(os.getenv("HOME") + "/.sdichecker") as file:
            for line in file:
                try:
                    (hostname, user, password) = line.rstrip("\n").split(" ", 3)
                    creds.add(hostname, user, password)
                except ValueError:
                    pass
    except FileNotFoundError:
        logger.info("No ~/.sdichecker file found, skipping credentials definition.")
        pass


def print_banner(args):
    logger.info("\nSDI check\n\n")
    logger.info("mode: %s\n", args.mode)
    if (args.mode == "CSW"):
        logger.info("metadata catalog CSW URL: %s", args.server)
    else:
        logger.info("WxS service URL: %s", args.server)
    logger.info("output mode: log")
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    logger.info("\n\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="the mode to consider (WMS, WFS, CSW)", choices=['WMS', 'WFS', 'CSW'])
    parser.add_argument("--inspire", help="indicates if the checks should be strict or flexible, default to flexible",
                        choices=['flexible', 'strict'], default="flexible")
    parser.add_argument("--server", help="the server to target (full URL, e.g. "
                                         "https://sdi.georchestra.org/geoserver/wms)")
    parser.add_argument("--geoserver-to-check", help="space-separated list of geoserver hostname to check in CSW mode. "
                                                     "Ex: sdi.georchestra.org", nargs="+")
    args = parser.parse_args(sys.argv[1:])
    load_credentials()

    if (args.mode is None or args.mode not in ["WMS", "WFS", "CSW"]):
        parser.print_help()
        sys.exit()

    print_banner(args)

    if (args.mode == "WMS" or args.mode == "WFS") and args.server is not None:
        logger.info("Querying %s ..." % args.server)
        ows_checker = None
        try:
            ows_checker = OwsChecker(args.server, wms=(True if args.mode == "WMS" else False), creds=creds)
            print_error(ows_checker.getInconsistencies())
            print(ows_checker.getReport())
        except BaseException as e:
            logger.info("Unable to parse the remote OWS server: %s", str(e))

    elif args.mode == "CSW" and args.server is not None:
        geoserver_services = CachedOwsServices(creds)
        try:
            csw_q = CSWQuerier(args.server, credentials=creds, cached_ows_services=geoserver_services)
        except ServiceException as e:
            logger.fatal("Unable to query the remote CSW:\nError: %s\nPlease check the CSW url", e)
            sys.exit(1)
        errors = []

        if args.inspire == "strict":
            for uuid in csw_q.get_service_mds():
                try:
                    csw_q.check_service_md(uuid, geoserver_to_check=args.geoserver_to_check)
                except Inconsistency as e:
                    errors.append(e)

        elif args.inspire == "flexible":
            while True:
                res = csw_q.get_records()
                # no more results, we should stop
                if csw_q.csw.results['returned'] == 0:
                    break
                for uuid in res:
                    logger.info("\nUUID : %s", uuid)
                    #        for uri in csw.records[uuid].uris:
                    for uri in csw_q.get_md(uuid).uris:
                        # print("%s %s %s" % (uri['protocol'], uri['url'], uri['name']))
                        try:
                            if uri["protocol"] == "OGC:WMS":
                                geoserver_services.checkWmsLayer(uri["url"], uri["name"])
                                logger.info("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                            elif uri["protocol"] == "OGC:WFS":
                                geoserver_services.checkWfsLayer(uri["url"], uri["name"])
                                logger.info("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                            else:
                                logger.info("\tSkipping URI : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                        except LayerNotFoundInconsistency as ex:
                            ex.set_md_uuid(uuid)
                            errors.append(ex)
                            logger.info("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s",
                                        uuid, uri['protocol'], uri['url'], uri['name'], ex)

