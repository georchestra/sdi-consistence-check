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
import re
import sys
import warnings
from time import localtime
from time import strftime
from urllib.parse import urlparse

from geoserver.catalog import Catalog
from mako.template import Template
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo

from GeonetworkToGeoserverUpdater import print_report
from credentials import Credentials
from cswquerier import CSWQuerier
from inconsistency import GsToGnUnableToCreateServiceMetadataInconsistency, Inconsistency
from utils import find_metadata
from bypassSSLVerification import bypassSSLVerification


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
    """
    Show the banner
    :param args: the argument object passed to the script.
    :return: nothing.
    """
    logger.info("\nGeoserver To Geonetwork Updater\n\n")
    logger.info("workspace to query: %s", args.workspace)
    logger.info("GeoServer: %s", args.geoserver)
    logger.info("GeoNetwork where to insert created metadata: %s", args.geonetwork)
    logger.info("dry-run: %s", args.dry_run)
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


def guess_geonetwork_url(url):
    """
    Guesses the geonetwork URL.
    :param url: the URL where the GeoNetwork base URL has to be guessed.
    :return: the url found.
    """
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
    # TODO 2 cases: either the MDD is not referenced onto a MDS (operatesOn), or the MDS does not exist yet
    # So, which strategy to adopt ?
    # * strategy #1: attempt to find a MDS, error if there is no MDS
    #
    # * strategy #2: gets all the MDS from the catalog, then finds the one which references the service
    # via the following xpath:
    # /gmd:MD_Metadata/gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/
    #     gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL
    # TODO: how to cache results ?
    csw_q = CSWQuerier(gn_cswurl)
    mds = csw_q.get_all_records(constraint=[PropertyIsEqualTo("Type", "service")])
    return mds


def create_service_metadata_from_template(data):
    """
        Returns a string suitable for a service metadata creation via CSW-T insert operation.

    :param data: a hashmap which has the following format:
    {
      'file_identifier': md_uuid,
      'current_date': "YYY-mm-dd",
      'service_name': the name of the GS workspace,
      'service_url': the service URL (see remark in comment above, should be used carefully
                     as identifier to get the MD),
      'current_datetime': "YYYY-mm-dd HH:mm",
      'abstract': abstract for the metadata,
      'layers': [
        'mdd_uuid': the uuid of the data metadata,
        'mdd_url': the URL of the data metadata,
        'name': the layer name
      ]
    }
    :return: a string representing the XML service metadata
    """
    return Template(filename="template/service-metadata.xml").render(**data)


def insert_metadata(gn_url, workspace, record, credentials=Credentials()):
    """
    Inserts a metadata in a catalogue, given its base URL.
    :param gn_url: the base URL to the GeoNetwork.
    :param workspace: the workspace name queried onto the GeoServer
    :param record: the metadata to be inserted
    :param credentials: the credentials object
    :return: True if success, raises an exception otherwise.
    """
    try:
        u = urlparse(gn_url)
        (username, password) = credentials.get(u.hostname)
        csw = CatalogueServiceWeb(gn_url + "/srv/eng/csw-publication", username=username, password=password)
        csw.transaction(ttype="insert", typename="gmd:MD_Metadata", record=record)
    except Exception as e:
        logger.error(e)
        raise GsToGnUnableToCreateServiceMetadataInconsistency(workspace, gn_url, e)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", help="indicates the GeoServer workspace name.", required=True)
    parser.add_argument("--geoserver", help="the GeoServer URL to use (e.g. 'http://localhost:8080/geoserver').", required=True)
    parser.add_argument("--geonetwork", help="the Geonetwork URL where the created metadatas have to be stored"
                                             " (e.g. 'http://localhost:8080/geonetwork').", required=True)
    parser.add_argument("--dry-run", help="Dry-run mode", action='store_true', default=False)
    parser.add_argument("--disable-ssl-verification", help="Disable certificate verification", action="store_true")

    args = parser.parse_args(sys.argv[1:])
    print_banner(args)

    if args.disable_ssl_verification:
        bypassSSLVerification()
    # Disable FutureWarning from owslib
    warnings.simplefilter("ignore", category=FutureWarning)

    # Load credentials
    creds = Credentials(logger=logger)
    (user, password) = creds.getFromUrl(args.geoserver)

    gscatalog = Catalog(args.geoserver + "/rest/", username=user, password=password)
    errors = []

    # Example of service MD publishing:
    #
    # data = {
    #            'file_identifier': '123456',
    #            'current_date': "YYY-mm-dd",
    #            'service_name': "sf",
    #            'service_url': 'http://localhost:8080/geoserver/sf/ows?service=wms',
    #            'current_datetime': "YYYY-mm-dd HH:mm",
    #                            'abstract': "Métadonnée de service WMS pour le workspace sf",
    #            'layers': [{
    #                 'mdd_uuid': "789101112",
    #                 'mdd_url': "http://localhost:8080/geonetwork/srv/eng/xml.metadata.get?uuid=789101112",
    #                 'name': 'roads'
    #            },
    #                {
    #                    'mdd_uuid': "yet-another-mdd",
    #                    'mdd_url': "http://localhost:8080/geonetwork/srv/eng/xml.metadata.get?uuid=yet-another-mdd",
    #                    'name': 'yet-another-mdd'
    #                },
    #            ]
    # }
    # new_service_md = create_service_metadata_from_template(data)
    # insert_metadata("http://localhost:8080/geonetwork", "sf",
    #                 new_service_md, Credentials(logger=logger))
    # sys.exit()

    workspace = gscatalog.get_workspace(name=args.workspace)
    if workspace is None:
        logger.error("workspace \"%s\" not found" % args.workspace)
        sys.exit()
    else:
        resources = gscatalog.get_resources(workspace=workspace)
        for res in resources:
            try:
                md_url, md = find_metadata(res, creds)
                layer = gscatalog.get_layer(name="%s:%s" % (res.workspace.name, res.name))
                linked_mds = guess_related_service_metadata(md_url, md)
                # TODO: add the missing logic for the current scenario here.
                # if linked_mds is None, then creates the MD, else ensures the
                # data MD can be found in the operatesOn from the returned service MD.
            except Inconsistency as e:
                errors.append(e)

    # cswquerier object will depend on the MD URL obtained from the GeoServer.
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
