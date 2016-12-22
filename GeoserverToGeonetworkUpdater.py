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
import re

from time import localtime
from time import strftime
from urllib.request import urlopen
import xml.etree.ElementTree as etree

from geoserver.catalog import Catalog
from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.fes import PropertyIsEqualTo, PropertyIsLike
from owslib.iso import MD_Metadata

from GeonetworkToGeoserverUpdater import print_report, guess_catalogue_endpoint
from cswquerier import CSWQuerier
from inconsistency import GsMetadataMissingInconsistency, Inconsistency


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
# TODO: define

def print_banner(args):
    logger.info("\nGeoserver To Geonetwork Updater\n\n")
    logger.info("workspace to query: %s", args.workspace)
    logger.info("GeoServer: %s", args.geoserver)
    logger.info("dry-run: %s", args.dry_run)
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


# TODO copy-pasted from GeonetworkToGeoserverUpdater, we need to find a way to share code across the codebase.
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


def guess_geonetwork_url(url):
    m = re.search('(.*\/geonetwork\/).*', url)
    return m.group(1)


def guess_related_service_metadata(md_url, md):
    """
    Retrieves the associated metadata from the same catalog (assuming it is a GeoNetwork)
    :param md_url: the original metadata Url
    :param md: the parsed metadata
    :return a list of service metadata:
    """
    gn_cswurl = "%ssrv/eng/csw" % guess_geonetwork_url(md_url)
    # TODO: Might be GeoNetwork-proprietary
    #
    # TODO 2: 2 cases: either the MDD is not referenced onto a MDS (operatesOn), or the MDS does not exist yet
    # So, which strategy to adopt ?
    # * strategy #1: attempt to find a MDS, error if there is no MDS
    #
    # * strategy #2: gets all the MDS from the catalog, then finds the one which references the service
    # via the following xpath:
    # /gmd:MD_Metadata/gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/
    #     gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL

    operates_on_filter = PropertyIsEqualTo('csw:operatesOnIdentifier', md.identifier)
    linkage_filter = PropertyIsLike('linkage', 'http://localhost:8080/geoserver/%')
    # TODO: see how to cache results ?
    csw_q = CSWQuerier(gn_cswurl)
    mds = csw_q.get_all_records(constraint=[PropertyIsEqualTo("Type", "service")])
    return mds

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
                md_url, md = find_metadata(res)
                layer = gscatalog.get_layer(name="%s:%s" % (res.workspace.name, res.name))
                linked_mds = guess_related_service_metadata(md_url, md)
                check_catalog(res, layer, args.dry_run)

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
