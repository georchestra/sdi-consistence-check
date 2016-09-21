#!/usr/bin/env python3
import logging
import os, sys

from owscheck import OwsChecker

logger = logging.getLogger("owschecker")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    wms_service_url = os.getenv('WMS_SERVICE')
    if not wms_service_url:
        sys.exit("Missing WMS_SERVICE environment variable")
    logger.info("Querying %s ..." % wms_service_url)
    owschecker = OwsChecker(wms_service_url, wms=False)
    owschecker.getReport()
