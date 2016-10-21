#!/usr/bin/env python3
import argparse
import logging
import os
import sys

from credentials import Credentials
from owscheck import OwsChecker

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="the mode to consider (WMS, WFS, CSW)")
    parser.add_argument("--server", help="the server to target (full URL, e.g. "
                                         "https://sdi.georchestra.org/geoserver/wms")
    args = parser.parse_args(sys.argv[1:])
    load_credentials()

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
        pass
    else:
        parser.print_help()