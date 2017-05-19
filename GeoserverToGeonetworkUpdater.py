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
import uuid
import warnings
import xml.etree.ElementTree as ET
from time import localtime
from time import strftime

from geoserver.catalog import Catalog
from mako.template import Template
from owslib.csw import CatalogueServiceWeb
from requests.exceptions import SSLError

from GeonetworkToGeoserverUpdater import print_report
from bypassSSLVerification import bypassSSLVerification
from credentials import Credentials
from cswquerier import CSWQuerier
from inconsistency import GsToGnUnableToCreateServiceMetadataInconsistency, Inconsistency, \
    GsToGnUnableToUpdateServiceMetadataInconsistency
from utils import find_data_metadata, print_report, load_workspaces_mapping


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
    logger.info("Workspace to query: %s", args.workspace)
    logger.info("GeoServer: %s", args.geoserver)
    logger.info("GeoNetwork where to insert created metadata: %s", args.geonetwork)
    logger.info("Dry-run: %s", args.dry_run)
    logger.info("\nStart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


def workspace_service_url(gs_url, workspace, service):
    return "%s/%s/ows?service=%s" % (gs_url, workspace, service)


def guess_related_service_metadata(gs_url, gn_url, workspace, service):
    """
    Retrieves the associated metadata from the same catalog (assuming it is a GeoNetwork)

    :param md: the parsed metadata
    :param gs_url: the GeoServer base URL
    :param gn_url: the catalogue base URL
    :param workspace: the workspace name
    :param service: the targeted service (WMS or WFS)
    :return the service metadata related to the service, if found
    """
    gn_cswurl = "%s/srv/eng/csw" % gn_url
    csw_q = CSWQuerier(gn_cswurl)
    mds = csw_q.get_all_records(constraint=[csw_q.is_service, csw_q.non_harvested])
    expected_service_url = workspace_service_url(gs_url, workspace, service)
    for smd_uuid, smd in mds.items():
        try:
            if smd.distribution.online[0].url == expected_service_url:
                return smd
        except:
            continue
    return None


def create_service_metadata_from_template(data, service_type):
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
      'title': a title for the metadata,
      'layers': [
        'mdd_uuid': the uuid of the data metadata,
        'mdd_url': the URL of the data metadata,
        'name': the layer name
      ]
    }
    :param service_type: the service type ('wms' or 'wfs')
    :return: a string representing the XML service metadata
    """
    return Template(filename="template/service-metadata-%s.xml" % (service_type)).render(**data)


def insert_metadata(gn_url, workspace, record, credentials=Credentials()):
    """
    Inserts a metadata in a catalogue, given its base URL.
    :param gn_url: the base URL to the GeoNetwork.
    :param workspace: the workspace name queried onto the GeoServer
    :param record: the metadata as string to be inserted
    :param credentials: the credentials object
    :return: True if success, raises an exception otherwise.
    """
    try:
        (username, password) = credentials.getFromUrl(gn_url)
        csw = CatalogueServiceWeb(gn_url + "/srv/eng/csw-publication", username=username, password=password)
        csw.transaction(ttype="insert", typename="gmd:MD_Metadata", record=record)
        return True
    except Exception as e:
        logger.error(e)
        raise GsToGnUnableToCreateServiceMetadataInconsistency(workspace, gn_url, e)


def update_metadata(gn_url, uuid, record, credentials=Credentials()):
    """
    Updates a metadata in the catalogue, given its UUID
    :param gn_url the GeoNetwork base url
    :param uuid the unique identifier of the metadata to be updated
    :param record the XML as string for the updated metadata
    :param credentials the Credentials object
    :return: True if success, raises an exception otherwise
    """
    try:
        (username, password) = credentials.getFromUrl(gn_url)
        csw = CatalogueServiceWeb(gn_url + "/srv/eng/csw-publication", username=username, password=password)
        csw.transaction(ttype="update", typename="gmd:MD_Metadata", record=record, identifier=uuid)
        return True
    except Exception as e:
        logger.error(e)
        raise GsToGnUnableToUpdateServiceMetadataInconsistency(workspace, uuid, gn_url, e)


def add_operates_on(xmlmd, md_url, uuidref):
    # /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn
    parsed_md = ET.fromstring(xmlmd)
    operates_on_elem = ET.Element('{http://www.isotc211.org/2005/srv}operatesOn')
    operates_on_elem.attrib["uuidref"] = uuidref
    operates_on_elem.attrib["{http://www.w3.org/1999/xlink}href"] = md_url
    parsed_md.find(".//{http://www.isotc211.org/2005/srv}SV_ServiceIdentification").append(operates_on_elem)
    return ET.tostring(parsed_md)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", help="indicates the GeoServer workspace name.", required=True)
    parser.add_argument("--geoserver", help="the GeoServer URL to use (e.g. 'http://localhost:8080/geoserver').", required=True)
    parser.add_argument("--geonetwork", help="the Geonetwork URL where the created metadatas have to be stored"
                                             " (e.g. 'http://localhost:8080/geonetwork').", required=True)
    parser.add_argument("--service", help="The service ('wfs' or 'wms')", choices=['wms', 'wfs'])
    parser.add_argument("--dry-run", help="Dry-run mode", action='store_true', default=False)
    parser.add_argument("--disable-ssl-verification", help="Disable certificate verification", action="store_true")
    parser.add_argument("--workspaces-mapping", help="the INI file to be loaded to resolve title and abstract on "
                                                     "created service metadata",
                        default="template/workspaces-mapping.ini.example")

    args = parser.parse_args(sys.argv[1:])
    if (args.workspace is None or args.geoserver is None or
                args.geonetwork is None or args.service not in ["wfs", "wms"]):
        parser.print_help()
        sys.exit()

    print_banner(args)

    if args.disable_ssl_verification:
        bypassSSLVerification()
    # Disable FutureWarning from owslib
    warnings.simplefilter("ignore", category=FutureWarning)

    # Load credentials
    creds = Credentials(logger=logger)
    (user, password) = creds.getFromUrl(args.geoserver)

    # Load the mapping file
    try:
        workspaces_mappings = load_workspaces_mapping(args.workspaces_mapping)
    except Exception as e:
        logger.error("Unable to load the mapping file for title/abstract.", e)
        sys.exit(1)

    gscatalog = Catalog(args.geoserver + "/rest/", username=user, password=password)
    errors = []
    try:
        workspace = gscatalog.get_workspace(name=args.workspace)
    except SSLError as e:
        logger.error("Unable to connect: SSL error (hint: use --disable-ssl-verification option)")
        sys.exit(1)

    if workspace is None:
        logger.error("Workspace \"%s\" not found" % args.workspace)
        sys.exit(1)

    else:
        resources = gscatalog.get_resources(workspace=workspace)
        for res in resources:
            try:
                # UUID from MDD is needed for operatesOn elements
                md_url, md = find_data_metadata(res, creds, args.disable_ssl_verification)
                layer = gscatalog.get_layer(name="%s:%s" % (res.workspace.name, res.name))
                linked_md = guess_related_service_metadata(args.geoserver, args.geonetwork,
                                                           args.workspace, args.service)
                if linked_md is None:
                    # Creates a new service metadata for the workspace
                    logger.info("No service metadata found for %s, creating one", args.workspace)
                    title = args.workspace
                    abstract = "Métadonnée de service décrivant " \
                        "l'espace de travail \"%s\" (%s)" % (args.workspace, args.service)
                    try:
                        title = workspaces_mappings[args.workspace]['title_' + args.service]
                        abstract = workspaces_mappings[args.workspace]['abstract_' + args.service]
                    except Exception as e:
                        logger.error("Unable to get a title or an abstract for created metadata, using default values")
                    data = {
                                'file_identifier': uuid.uuid4(),
                                'current_date': strftime("%Y-%m-%d", localtime()),
                                'service_name': args.workspace,
                                'service_url': workspace_service_url(args.geoserver, args.workspace, args.service),
                                'current_datetime': strftime("%Y-%m-%d %H:%M:%S", localtime()),
                                'abstract': abstract,
                                'title': title,
                                'layers': []
                    }
                    # reiterates on the resources object, to first create the complete service metadata
                    # (with all the linked data MD).
                    # this should not harm since we don't touch to the array, and access it only in a read-only mode
                    for r2 in resources:
                        try:
                            r2mdurl, r2md = find_data_metadata(r2, creds, args.disable_ssl_verification)
                            data['layers'].append({
                                'mdd_uuid': r2md.identifier,
                                'mdd_url': r2mdurl,
                                'name': r2.name,
                                'title': r2.title
                            })
                        except:
                            logger.error("Unable to find all the data metadata for some layers on workspace '%s',"
                                         " generated service metadata might be incomplete.", args.workspace)
                            logger.error("Layer with missing data metadata URL: '%s:%s'", args.workspace,
                                         r2.name)
                    new_srv_md = create_service_metadata_from_template(data, args.service)
                    if not args.dry_run:
                        insert_metadata(args.geonetwork, args.workspace, new_srv_md, creds)
                    else:
                        logger.info("Dry-run: would have created a service metadata for workspace '%s'"
                                    " and metadata '%s'", args.workspace, md.identifier)
                else:
                    # service metadata found for the current workspace
                    # we still have to check if the mds references (operatesOn)
                    # all the MDD defined in the layers' workspace.
                    operateson_found = False
                    for oon in linked_md.serviceidentification.operateson:
                        if oon['uuidref'] == md.identifier:
                            operateson_found = True
                            break
                    if not operateson_found:
                        logger.info("MDD '%s' is not referenced into the service MD '%s' (on server '%s'). "
                                    "Adding the operatesOn link", md_url, linked_md.identifier, args.geonetwork)
                        xml_mds = add_operates_on(linked_md.xml, md_url, md.identifier)
                        if not args.dry_run:
                            update_metadata(args.geonetwork, linked_md.identifier, xml_mds, creds)
                        else:
                            logger.info("Dry-run: would have updated md %s [adding operatesOn on data md %s]",
                                        linked_md.identifier, md.identifier)
                    # TODO: also check / fix SV_CoupledResource ?

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

    print_report(logger, errors)
