#!/usr/bin/env python3
import argparse
import logging
import warnings
import sys
from math import floor
from time import strftime, localtime

from owslib.util import ServiceException

from credentials import Credentials
from cswquerier import CachedOwsServices, CSWQuerier
from inconsistency import Inconsistency, GnToGsLayerNotFoundInconsistency, GnToGsNoOGCWmsDefined, GnToGsNoOGCWfsDefined, \
    GnToGsOtherError, GnToGsInvalidCapabilitiesUrl
from owscheck import OwsChecker
from bypassSSLVerification import bypassSSLVerification

# Logging configuration
logger = logging.getLogger("owschecker")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


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


def print_layers_status(owschecker):
    errors = owschecker.get_inconsistencies()
    layers = owschecker.get_layer_names()
    layers_in_error = [ error.layer_index for error in errors ]
    curr_idx = 0
    for idx, error in enumerate(errors):
        while curr_idx < error.layer_index:
            if curr_idx not in layers_in_error:
                logger.info("#%d\n  Layer: %s OK\n", curr_idx, layers[curr_idx])
            curr_idx += 1
        logger.error("#%d\n  Layer: %s", error.layer_index, error.layer_name)
        logger.error("  Error: %s\n" % str(error))


def print_ows_report(owschecker):
        total_layers = len(owschecker.get_layer_names())
        inconsistencies = owschecker.get_inconsistencies()
        layers_error = set()
        for inconst in inconsistencies:
            layers_error.add(inconst.layer_index)
        inconsistencies_found = len(layers_error)
        layers_inconst_percent = floor((inconsistencies_found * 100 / total_layers)) if \
            total_layers > 0 else 0
        logger.info("\n\n%d layers parsed, %d inconsistencies found (%d %%)", total_layers,
                    inconsistencies_found, layers_inconst_percent)
        logger.info("end time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))


def print_csw_report(errors, total_mds):
    unique_mds_in_error = { error.md_uuid for error in errors }
    err_percent = floor(len(unique_mds_in_error) * 100 / total_mds) if total_mds > 0 else 0
    logger.info("\n\n%d metadata parsed, %d inconsistencies found, %d unique metadatas in error (%d %%)",
                total_mds, len(errors), len(unique_mds_in_error), err_percent)
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

    parser.add_argument("--only-err", help="Only display errors, no summary informations will be displayed",
                        action="store_true")

    args = parser.parse_args(sys.argv[1:])
    creds = Credentials(logger=logger)

    if args.disable_ssl_verification:
        bypassSSLVerification()
    # Disable FutureWarning from owslib
    warnings.simplefilter("ignore", category=FutureWarning)

    if not args.only_err:
        print_banner(args)

    if (args.mode == "WMS" or args.mode == "WFS") and args.server is not None:
        logger.debug("Querying %s ..." % args.server)
        ows_checker = None
        try:
            ows_checker = OwsChecker(args.server, wms=(True if args.mode == "WMS" else False), creds=creds)
            logger.debug("Finished integrity check against %s GetCapabilities", args.mode)
            print_layers_status(ows_checker)
            if not args.only_err:
                print_ows_report(ows_checker)
        except BaseException as e:
            logger.info("Unable to parse the remote OWS server: %s", str(e))

    elif args.mode == "CSW" and args.server is not None:
        total_mds = 0
        geoserver_services = CachedOwsServices(creds, disable_ssl=args.disable_ssl_verification)
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
            global_idx = 0
            while True:
                res = csw_q.get_dataset_records()
                if (len(res)) == 0:
                    break
                total_mds += len(res)
                for idx, uuid in enumerate(res):
                    current_md = res[uuid]
                    logger.info("#%d\n  UUID : %s\n  %s", global_idx, uuid, current_md.title)
                    wms_found = False
                    wfs_found = False
                    for uri in csw_q.get_md(uuid).uris:
                        from_wms = False
                        try:
                            if uri["protocol"] == "OGC:WMS":
                                wms_found = True
                                from_wms = True
                                # TODO: use the geoserver_to_check option ?
                                geoserver_services.checkWmsLayer(uri["url"], uri["name"])

                                logger.debug("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                                logger.info("    WMS url: OK")
                            elif uri["protocol"] == "OGC:WFS":
                                wfs_found = True
                                # TODO: same remark
                                geoserver_services.checkWfsLayer(uri["url"], uri["name"])
                                logger.debug("\tURI OK : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                                logger.info("    WFS url: OK")
                            else:
                                logger.debug("\tSkipping URI : %s %s %s", uri["protocol"], uri['url'], uri['name'])
                        except BaseException as ex:
                            if isinstance(ex, GnToGsLayerNotFoundInconsistency) or \
                                isinstance(ex, GnToGsInvalidCapabilitiesUrl) or    \
                                            isinstance(ex,GnToGsOtherError):
                                ex.set_md_uuid(uuid)
                                errors.append(ex)
                            else:
                                # morph encountered error in to an "other error"
                                exc = GnToGsOtherError(uri['url'], uri['name'], ex)
                                exc.set_md_uuid(uuid)
                                errors.append(exc)
                            logger.debug("\t /!\\ ---> Cannot find Layer ON GS : %s %s %s %s %s",
                                        uuid, uri['protocol'], uri['url'], uri['name'], ex)
                            logger.info("    %s url: KO: %s: %s" % ("WMS" if from_wms else "WFS",
                                                                    uri['url'], str(errors[-1])))
                    if not wms_found:
                        logger.info("    WMS url: KO: No wms url found in the metadata")
                        errors.append(GnToGsNoOGCWmsDefined(uuid))
                    if not wfs_found:
                        logger.info("    WFS url: KO: No wfs url found in the metadata")
                        errors.append(GnToGsNoOGCWfsDefined(uuid))
                    logger.info("")
                    # end of current md
                    global_idx += 1
                if csw_q.csw.results['nextrecord'] == 0:
                    break
        print_csw_report(errors, total_mds)