#!/usr/bin/env python3
import logging
import os
import sys
from getpass import getpass

from credentials import Credentials
from owscheck import OwsChecker

logger = logging.getLogger("owschecker")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


def print_error(errors):
    for error in errors:
        print("Error : %s" % str(error))

if __name__ == "__main__":
    creds = Credentials()
    creds.add("sdi.georchestra.org", "xxx", getpass())
    wms_service_url = os.getenv('WMS_SERVICE')
    if not wms_service_url:
        sys.exit("Missing WMS_SERVICE environment variable")
    logger.info("Querying %s ..." % wms_service_url)
    ows_checker = None
    try:
        ows_checker = OwsChecker(wms_service_url, wms=True, creds=creds)
        print_error(ows_checker.getInconsistencies())
        print(ows_checker.getReport())

    except BaseException as e:
        logger.info("Unable to parse the remote OWS server: %s", str(e))

