#
# scénario 3: Synchronisation Geoserver vers GeoNetwork
#
# en entrée: nom d'espace de travail GS (et url du GeoServer)
# Parcours des couches de l'espace de travail
#   Si aucune metadataURL, remonter la couche en erreur
#   Sinon, aller sur GN et récupérer la MDD, puis les MDS du workspace
#     Si celles-ci n'existent pas, les créer.
#
# Problemes:
#
# * la MDS risque d'etre celle globale à tout le GeoServer, le cahier des charges prévoit
#   de créer 2 métadonnées de service par espace de travail (une pour WMS et une pour WFS). Cela risque de donner
#   deux fiches quasiment identiques, mais aussi quasiment vides. (les seules infos que l'on ait sur un workspace coté
#   geoserver étant le nom et son URL de namespace).
#
# * Il faut pouvoir discrimer ces nouvelles MDs afin de les retrouver par lancement successifs. Ce n'est pas vraiment
#   faisable en l'état sans adopter une convention. Disons que l'URL du service doit etre de la forme (commencer par,
#   sans respecter la casse):
#   - http://url-geoserver/geoserver/nom-workspace/ows?service=wms pour wms
#   - http://url-geoserver/geoserver/nom-workspace/ows?service=wfs pour wfs
#
import argparse
import logging

import sys
from time import localtime
from time import strftime
from urllib.request import urlopen
import xml.etree.ElementTree as etree

from geoserver.catalog import Catalog
from owslib.iso import MD_Metadata

from GeonetworkToGeoserverUpdater import print_report
from cswquerier import CSWQuerier
from inconsistency import GsMetadataMissingInconsistency, Inconsistency

GS_URL = "http://localhost:8080/geoserver"
GN_URL = "http://localhost:8080/geonetwork"
WS_NAME = "sf"

def init_mdd_mds_mapping(cswQuerier):
  mds = cswQuerier.get_service_mds()
  mdd_to_mds = {}
  for mduuid, md in mds.items():
      for mdd in md.serviceidentification.operateson:
          uuidref = mdd["uuidref"]
          if mdd_to_mds.get(uuidref, None) is None:
              mdd_to_mds[uuidref] = []
          mdd_to_mds[uuidref].append(mduuid)
  return mdd_to_mds


# Logging configuration
logger = logging.getLogger("GsToGnUpdater")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)

# these variables are global to have a hand easily on cached resources obtained remotely


# See https://github.com/georchestra/georchestra/issues/756#issuecomment-58194935
# on how to get MDs from MDD uuid via a CSW getRecords operation

def print_banner(args):
    logger.info("\nGeoserver To Geonetwork Updater\n\n")
    logger.info("workspace to query: %s", args.workspace)
    logger.info("GeoServer: %s", args.geoserver)
    logger.info("dry-run: %s", args.dry_run)
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


# TODO copy-pasted from GeonetworkToGeoserverUpdater, need to find a way to share code across the codebase.
def find_metadata(resource):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :return: a tuple (url, parsed metadata).
    """
    if resource.metadata_links is None:
        raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)
    for mime_type, format, url in resource.metadata_links:
        if mime_type == "text/xml" and format == "ISO19115:2003":
            with urlopen(url) as fhandle:
                return (url, MD_Metadata(etree.parse(fhandle)))
    raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)

def check_catalog(res, layer, dry_run):
    logger.info("%s", layer.name)
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", help="""indicates the GeoServer workspace name.""")
    parser.add_argument("--geoserver", help="the GeoServer URL to use (e.g. 'http://localhost:8080/geoserver').")
    parser.add_argument("--dry-run", help="Dry-run mode", action='store_true')
    parser.set_defaults(dry_run=False)

    args = parser.parse_args(sys.argv[1:])
    if (args.workspace is None or args.geoserver is None):
        parser.print_help()
        sys.exit()

    print_banner(args)

    gscatalog = Catalog(args.geoserver + "/rest/")
    errors = []

    workspace = gscatalog.get_workspace(name=args.workspace)
    if workspace is None:
        logger.error("workspace \"%s\" not found" % args.workspace)
        sys.exit()
    else:
        resources = gscatalog.get_resources(workspace=workspace)
        for res in resources:
            try:
                md = find_metadata(res)
                print(md)

            except Inconsistency as e:
                errors.append(e)

    # cswquerier object will depend on the MD URL obtained from the GeoServer
    # We can imagine having more than one catalogue instance, then we need
    # to find a way to keep the MDD to MDS mapping for each of these catalogues.
    #
    #csw_q = None
    #servicesmd = csw_q.get_all_records(constraint=csw_q.is_service)
    #data_to_service_map = {}
    #for uuid, md in servicesmd.items():
    #    for oon in md.identificationinfo[0].operateson:
    #        if data_to_service_map.get(oon['uuidref']) is None:
    #            data_to_service_map[oon['uuidref']] = [uuid]
    #        else:
    #            data_to_service_map[oon['uuidref']] = data_to_service_map[oon['uuidref']] + [uuid]

    print_report(errors)
