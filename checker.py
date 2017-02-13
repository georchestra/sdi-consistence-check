#!/usr/bin/env python3
import argparse
import logging
import warnings
import os
import sys
from math import floor
from time import strftime, localtime

from owslib.util import ServiceException

from credentials import Credentials
from cswquerier import CachedOwsServices, CSWQuerier
from inconsistency import Inconsistency, GnToGsLayerNotFoundInconsistency, GnToGsNoOGCWmsDefined, GnToGsNoOGCWfsDefined
from owscheck import OwsChecker
from bypassSSLVerification import bypassSSLVerification

# Logging configuration
logger = logging.getLogger("owschecker")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


def print_layers_error(errors):
    for idx, error in enumerate(errors):
        logger.error("#%d\n  Layer: %s", idx, error.layer_name)
        logger.error("  Error: %s\n" % str(error))

def print_banner(args):
    logger.info("\nSDI check\n\n")
    logger.info("mode: %s\n", args.mode)
    if (args.mode == "CSW"):
        logger.info("metadata catalog CSW URL: %s", args.server)
        logger.info("INSPIRE mode: %s", args.inspire)
    else:
        logger.info("WxS service URL: %s", args.server)
    logger.info("output mode: log")
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


def print_ows_report(owschecker):
        total_layers = sum(len(v) for k, v in owschecker.get_service().layersByWorkspace.items())
        inconsistencies_found = len(owschecker.get_inconsistencies())
        layers_inconst_percent = floor((inconsistencies_found * 100 / total_layers)) if \
            total_layers > 0 else 0
        logger.info("\n\n%d layers parsed, %d inconsistencies found (%d %%)", total_layers,
                    inconsistencies_found, layers_inconst_percent)
        logger.info("end time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))


def print_csw_report(errors, total_mds):
    unique_mds_in_error = { error.md_uuid for error in errors }
    err_percent = floor(len(unique_mds_in_error) * 100 / total_mds) if total_mds > 0 else 0
    logger.info("\n\n%d metadata parsed, %d inconsistencies found (%d %%)",
                total_mds, len(errors), err_percent)
    logger.info("end time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="the mode to consider (WMS, WFS, CSW)",
                        choices=['WMS', 'WFS', 'CSW'], required=True)
    parser.add_argument("--inspire", help="indicates if the checks should be strict or flexible, default to flexible",
                        choices=['flexible', 'strict'], default="flexible")
    parser.add_argument("--server", help="the server to target (full URL, e.g. "
                                         "https://sdi.georchestra.org/geoserver/wms)")
    parser.add_argument("--geoserver-to-check", help="space-separated list of geoserver hostname to check in CSW mode "
                                                     "with inspire strict option activated. "
                                                     "Ex: sdi.georchestra.org", nargs="+")
    parser.add_argument("--disable-ssl-verification", help="Disable certificate verification", action="store_true")

    args = parser.parse_args(sys.argv[1:])
    creds = Credentials(logger=logger)

    if args.disable_ssl_verification:
        bypassSSLVerification()
    # Disable FutureWarning from owslib
    warnings.simplefilter("ignore", category=FutureWarning)

    print_banner(args)

    if (args.mode == "WMS" or args.mode == "WFS") and args.server is not None:
        logger.debug("Querying %s ..." % args.server)
        ows_checker = None
        try:
            ows_checker = OwsChecker(args.server, wms=(True if args.mode == "WMS" else False), creds=creds)
            logger.debug("Finished integrity check against %s GetCapabilities", args.mode)
            print_layers_error(ows_checker.get_inconsistencies())
            print_ows_report(ows_checker)
        except BaseException as e:
            logger.info("Unable to parse the remote OWS server: %s", str(e))

    elif args.mode == "CSW" and args.server is not None:
        total_mds = 0
        geoserver_services = CachedOwsServices(creds)
        try:
            csw_q = CSWQuerier(args.server, credentials=creds, cached_ows_services=geoserver_services,
                               logger=logger)
        except ServiceException as e:
            logger.fatal("Unable to query the remote CSW:\nError: %s\nPlease check the CSW url", e)
            sys.exit(1)
        errors = []

        if args.inspire == "strict":
            # Step 1: get all data metadata
            datamd = csw_q.get_all_records(constraint=[csw_q.is_dataset])
            # Step 2: maps data metadatas to service MDs
            servicesmd = csw_q.get_all_records(constraint=[csw_q.is_service])
            data_to_service_map = {}
            for uuid, md in servicesmd.items():
                for oon in md.identificationinfo[0].operateson:
                    if data_to_service_map.get(oon['uuidref']) is None:
                        data_to_service_map[oon['uuidref']] = [uuid]
                    else:
                        data_to_service_map[oon['uuidref']] = data_to_service_map[oon['uuidref']] + [uuid]

            # Step 3: on each data md, get the service md, and the underlying service URL
            #for uuid, md in enumerate(datamd):
            for mdd_uuid, mdd in datamd.items():
                # Note: this won't count the service metadata in the end, only the MDD that trigger a
                # check onto a service MD.
                total_mds += 1
                if data_to_service_map.get(mdd_uuid) is None:
                    # TODO file an issue if the dataMd as no ServiceMd linked to ?
                    continue
                # step 4: check the layer existence using the service URL
                for sce_uuid in data_to_service_map[mdd_uuid]:
                    try:
                        mds = servicesmd[sce_uuid]
                        mdd = datamd[mdd_uuid]
                        csw_q.check_service_md(mds, mdd, geoserver_to_check=args.geoserver_to_check if
                                               args.geoserver_to_check is not None else [])
                    except Inconsistency as e:
                        logger.error(e)
                        errors.append(e)

        elif args.inspire == "flexible":
            while True:
                res = csw_q.get_dataset_records()
                total_mds += len(res)
                for idx, uuid in enumerate(res):
                    current_md = res[uuid]
                    logger.info("#%d\n  UUID : %s\n  %s", idx, uuid, current_md.title)
                    wms_found = False
                    wfs_found = False
                    for uri in csw_q.get_md(uuid).uris:
                        try:
                            if uri["protocol"] == "OGC:WMS":
                                wms_found = True
                                # TODO: use the geoserver_to_check option ?
                                geoserver_services.checkWmsLayer(uri["url"], uri["name"])
                                logger.debug("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                            elif uri["protocol"] == "OGC:WFS":
                                wfs_found = True
                                # TODO: same remark
                                geoserver_services.checkWfsLayer(uri["url"], uri["name"])
                                logger.debug("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                            else:
                                logger.debug("\tSkipping URI : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                        except GnToGsLayerNotFoundInconsistency as ex:
                            ex.set_md_uuid(uuid)
                            errors.append(ex)
                            logger.debug("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s",
                                        uuid, uri['protocol'], uri['url'], uri['name'], ex)

                    str_wms_found = "    checking WMS url: "
                    str_wfs_found = "    checking WFS url: "
                    if wms_found == False:
                        str_wms_found += "error: no OGC:WMS url defined"
                        errors.append(GnToGsNoOGCWmsDefined(uuid))
                    else:
                        str_wms_found += "OK"
                    if wfs_found == False:
                        str_wfs_found += "error: no OGC:WFS url defined"
                        errors.append(GnToGsNoOGCWfsDefined(uuid))
                    else:
                        str_wfs_found += "OK"
                    logger.info(str_wms_found)
                    logger.info(str_wfs_found)
                    logger.info("")
                # no more results, we should stop
                if csw_q.csw.results['nextrecord'] == 0:
                    break

        print_csw_report(errors, total_mds)