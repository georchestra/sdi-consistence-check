#!/usr/bin/env python3
import argparse
import os
import logging
import warnings
import sys
from math import floor
from time import strftime, localtime
import xml.etree.cElementTree as ET

from owslib.util import ServiceException

from credentials import Credentials
from cswquerier import CachedOwsServices, CSWQuerier
from inconsistency import Inconsistency, GnToGsLayerNotFoundInconsistency, GnToGsNoOGCWmsDefined, GnToGsNoOGCWfsDefined, \
    GnToGsOtherError, GnToGsInvalidCapabilitiesUrl
from owscheck import OwsChecker
from bypassSSLVerification import bypassSSLVerification



def print_banner(args):
    logger.info("\nSDI check\n\n")
    logger.info("mode: %s\n", args.mode)
    if args.mode == "CSW":
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

def generate_ows_xunit_layers_status(owschecker, output_file):
    errors = owschecker.get_inconsistencies()
    layers = owschecker.get_layer_names()
    layers_in_error = [ error.layer_index for error in errors ]
    curr_idx = 0
    results = []
    for idx, error in enumerate(errors):
        while curr_idx < error.layer_index:
            if curr_idx not in layers_in_error:
                # Layer OK
                results.append({ "classname": "WMS" if owschecker.wms else "WFS", "name": layers[curr_idx], "time": "0", "error": None })
            curr_idx += 1
        results.append({ "classname": "WMS" if owschecker.wms else "WFS", "name": layers[curr_idx], "time": "0", "error": error})
    nberrors = sum(1 for i in results if i['error'] is not None)
    root = ET.Element("testsuite", {"name": "sdi-consistence-checker",
        "tests": str(len(results)), "errors": str(nberrors), "failures": "0", "skip": "0" })
    for result in results:
        error = result.pop('error')
        tcase = ET.SubElement(root, "testcase", result)
        if error is not None:
            ET.SubElement(tcase, "error", { "type": type(error).__name__, "message": str(error) }).text = str(error)
    tree = ET.ElementTree(root)
    tree.write(output_file)

def generate_csw_xunit_layers_status(results, output_file):
    """
      Generates a xunit report for CSW analysis.
      @param results an array containing the xml attributes to add to the testcase elements,
             plus uuid and error (which will have to be popped before adding)
      @param output_file the XML output filename to be generated, defaults to xunit.xml
    """
    nberrors = sum(1 for i in results if i['error'] is not None)
    root = ET.Element("testsuite", {"name": "sdi-consistence-checker",
        "tests": str(len(results)), "errors": str(nberrors), "failures": "0", "skip": "0" })
    for result in results:
        error = result.pop('error')
        current_uuid = result.pop("uuid")
        tcase = ET.SubElement(root, "testcase", result)
        if error is not None:
            ET.SubElement(tcase, "error", { "type": type(error).__name__, "message": str(error) }).text = str(error)
    tree = ET.ElementTree(root)
    tree.write(output_file)

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

    parser.add_argument("--check-layers", help="check WMS/WFS layer validity", action="store_true")

    parser.add_argument("--only-err", help="Only display errors, no summary informations will be displayed",
                        action="store_true")

    parser.add_argument("--xunit", help="Generate a XML xunit result report",
                        action="store_true")

    parser.add_argument("--xunit-output", help="Name of the xunit report file, defaults to ./xunit.xml", default="xunit.xml")

    parser.add_argument("--log-to-file", help="If a file path is specified, log output to this file, not stdout")

    parser.add_argument("--timeout", type=int, help="Specify a timeout for request to external service.")

    args = parser.parse_args(sys.argv[1:])

    logger = logging.getLogger("owschecker")
    hdlr = logging.FileHandler(args.log_to_file, mode='w') if args.log_to_file is not None \
        else logging.StreamHandler(sys.stdout)
    hdlr.setLevel(logging.INFO)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    creds = Credentials(logger=logger)

    request_timeout = args.timeout or int(os.getenv('REQUEST_TIMEOUT', 30))

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
            ows_checker = OwsChecker(args.server, wms=(True if args.mode == "WMS" else False),
                                     creds=creds, checkLayers = (args.check_layers != None),
                                     timeout=request_timeout)
            logger.debug("Finished integrity check against %s GetCapabilities", args.mode)
            print_layers_status(ows_checker)
            if not args.only_err:
                print_ows_report(ows_checker)
            if args.xunit:
                    generate_ows_xunit_layers_status(ows_checker, args.xunit_output)
        except BaseException as e:
            logger.info("Unable to parse the remote OWS server: %s", str(e))

    elif args.mode == "CSW" and args.server is not None:
        total_mds = 0
        geoserver_services = CachedOwsServices(creds,
                                               disable_ssl=args.disable_ssl_verification,
                                               timeout=request_timeout)
        try:
            csw_q = CSWQuerier(args.server, credentials=creds, cached_ows_services=geoserver_services, logger=logger, timeout=request_timeout)
        except ServiceException as e:
            logger.fatal("Unable to query the remote CSW:\nError: %s\nPlease check the CSW url", e)
            sys.exit(1)
        errors = []
        reporting = []
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
                    # TODO file an issue if the dataMd has no ServiceMd linked to ?
                    if len([x for x in reporting if x['uuid'] == mdd_uuid]) == 0:
                        reporting.append({ 'classname': 'CSW', 'name': mdd.identification.title, 'uuid': mdd_uuid,
                              'time': '0', 'error': None })
                    continue
                # step 4: check the layer existence using the service URL
                for sce_uuid in data_to_service_map[mdd_uuid]:
                    try:
                        mds = servicesmd[sce_uuid]
                        mdd = datamd[mdd_uuid]
                        csw_q.check_service_md(mds, mdd, geoserver_to_check=args.geoserver_to_check if
                                               args.geoserver_to_check is not None else [])
                        # No issue so far ?
                        # since a MDD can reference several service metadata, consider
                        # the MDD as passing tests only once (avoid adding several times the same MDD
                        # to the array). It must be very unlikely to have several MDS anyway.
                        if len([x for x in reporting if x['uuid'] == mdd_uuid]) == 0:
                            reporting.append({ 'classname': 'CSW', 'name': mdd.title, 'uuid': mdd_uuid,
                              'time': '0', 'error': None })
                    except Inconsistency as e:
                        logger.error(e)
                        errors.append(e)
                        # Same as above: only adding the errored MDD once
                        if len([x for x in reporting if x['uuid'] == mdd_uuid]) == 0:
                            reporting.append({ 'classname': 'CSW', 'name': mdd.title, 'uuid': mdd_uuid,
                              'time': '0', 'error': e })

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
                            # in both cases, add the MDD in the reporting array
                            if len([x for x in reporting if x['uuid'] == uuid]) == 0:
                                reporting.append({ 'classname': 'CSW', 'name': current_md.title, 'uuid': uuid,
                                    'time': '0', 'error': ex })

                    if not wms_found:
                        logger.info("    WMS url: KO: No wms url found in the metadata")
                        err = GnToGsNoOGCWmsDefined(uuid)
                        errors.append(err)
                        reporting.append({ 'classname': 'CSW', 'name': current_md.title, 'uuid': uuid,
                                    'time': '0', 'error': err })

                    if not wfs_found:
                        logger.info("    WFS url: KO: No wfs url found in the metadata")
                        err = GnToGsNoOGCWfsDefined(uuid)
                        errors.append(err)
                        reporting.append({ 'classname': 'CSW', 'name': current_md.title, 'uuid': uuid,
                                    'time': '0', 'error': err })
                    if wms_found and wfs_found:
                        reporting.append({ 'classname': 'CSW', 'name': current_md.title, 'uuid': uuid,
                                    'time': '0', 'error': None })
                    logger.info("")
                    # end of current md
                    global_idx += 1
                if csw_q.csw.results['nextrecord'] == 0:
                    break
        print_csw_report(errors, total_mds)
        if args.xunit:
            generate_csw_xunit_layers_status(reporting, args.xunit_output)
