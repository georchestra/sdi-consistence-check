import argparse
import base64
import logging

import sys
from time import strftime, localtime
from urllib.request import urlopen, Request

import re
from geoserver.catalog import Catalog
from owslib.etree import etree
from owslib.iso import MD_Metadata

from credentials import Credentials
from cswquerier import CSWQuerier
from bypassSSLVerification import bypassSSLVerification

# Scénario 2 Read-Write GN -> GS
#
# 1. récupérer sur le GS les couches concernées par le lancement (parametres)
# * remonter une erreur si la couche ne référence pas de MD
# 2. Remonter sur GN, et récupérer la MDD référencée
# 3. Modifier si nécessaire les champs suivants:
# * Titre
# * résumé
# * url en html ? (TODO)
# * Attribution (récupérer le useLimitation, et regexp sur "(.*)")
#   md.identificationinfo[0].uselimitation[0]
#

from inconsistency import GsMetadataMissingInconsistency, Inconsistency

# Logging configuration
logger = logging.getLogger("GnToGsUpdater")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


def update_resource(layer, resource, title, abstract, md_url_html, attribution, dry_run):
    """
    Updates a Geoserver resource
    :param layer: the gsconfig layer object
    :param resource: a gsconfig resource object
    :param title: the title to set
    :param abstract: the abstract to set
    :param md_url_html: the metadata url for the HTML version
    :param attribution: the text describing the attribution for the resource
    :param dry_run: true does not modify anything, false for actually saving the resource
    :return:
    """
    # Updates the MD title (if not present or if the MD title is bigger)
    upd_title = False
    upd_abstract = False
    upd_attribution = False
    if resource.title is None or len(resource.title) < len(title):
        resource.title = title
        upd_title = True
    # Same algo for the abstract
    if resource.abstract is None or len(resource.abstract) < len(abstract):
        resource.abstract = abstract
        upd_abstract = True
    if layer.attribution is None or layer.attribution['title'] is None \
            or len(layer.attribution["title"]) < len(attribution):
        upd_attribution = True
        if layer.attribution is None:
            layer.attribution = {"title": attribution}
        else:
            attribs = layer.attribution
            attribs["title"] = attribution
            layer.attribution = attribs
    # Check that MD Urls are present
    has_md_html = False
    # Note: res.metadata_links cannot be None, because we used it to get the MDD
    mdlinks = resource.metadata_links
    for lnk in mdlinks:
        if lnk[0] == "text/html":
            has_md_html = True
            break
    if not has_md_html:
        mdlinks.append(("text/html", "ISO19115:2003", md_url_html))
    if not dry_run:
        # to trigger an update of the MDs, I guess the array should be re-affected
        # (so that the object is considered as dirty / update needed against the GS REST API)
        resource.metadata_links = mdlinks
        catalog = resource.catalog
        catalog.save(resource)
        catalog.save(layer)
        catalog.reload()
        logger.info("\"%s:%s\": layer / resource info updated\n", resource.workspace.name, resource.name)
    else:
        logger.info("dry-run mode: not updating the resource for layer \"%s\"" % resource.title)
        if upd_title:
            logger.info("\t- the title of the resource should have been updated")
        if upd_abstract:
            logger.info("\t- the abstract of the resource should have been updated")
        if upd_attribution:
            logger.info("\t- the attribution of the layer should have been updated")
        if not has_md_html:
            logger.info("\t- an HTML metadata URL should have been added")
        logger.info("\n")


def find_metadata(resource, credentials):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :return: a tuple (url, parsed metadata).
    """
    if resource.metadata_links is None:
        raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)
    for mime_type, format, url in resource.metadata_links:
        if mime_type == "text/xml" and format == "ISO19115:2003":
            # disable certificate verification
            # ctx = ssl.create_default_context()
            # ctx.check_hostname = False
            # ctx.verify_mode = ssl.CERT_NONE
            req = Request(url)
            username, password = credentials.getFromUrl(url)
            if username is not None:
                base64string = base64.b64encode(('%s:%s' % (username, password)).encode())
                authheader =  "Basic %s" % base64string.decode()
                req.add_header("Authorization", authheader)
                logger.debug("Adding credential for %s : %s" % (url, username))

            with urlopen(req) as fhandle:
                return (url, MD_Metadata(etree.parse(fhandle)))
    raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)


def guess_catalogue_endpoint(url, md_identifier):
    """
    Given a URL, try to guess the catalogue endpoint. This method is used to guess the HTML URL for the metadata.
    This is for now meant to work only with GeoNetwork (which is the catalogue mainly used in geOrchestra).

    :param url: the metadata URL
    :param md_identifier: the unique identifier of the metadata
    :return: the guessed url.
    """
    m = re.search('(.*\/geonetwork\/).*', url)
    return "%s?uuid=%s" % (m.group(1), md_identifier)


def extract_attribution(str):
    try:
        m = re.search('"(.*)"', str)
        return m.group(1)
    except:
        logger.error("unable to extract the attribution, using the whole otherConstraint field")
        return str


def gn_to_gs_fix(layer, resource, dry_run, credentials):
    url, md = find_metadata(resource, credentials)
    md_title = md.identificationinfo[0].title if len(md.identificationinfo) > 0 else ""
    md_abstract = md.identificationinfo[0].abstract if len(md.identificationinfo) > 0 else ""
    md_url_html = guess_catalogue_endpoint(url, md.identifier)
    md_attribution = extract_attribution(md.identification.otherconstraints[0]) \
        if (len(md.identification.otherconstraints)) > 0 else ""
    update_resource(layer, resource, md_title, md_abstract, md_url_html, md_attribution, dry_run)


def print_banner(args):
    logger.info("\nGeoNetwork To Geoserver Updater\n\n")
    logger.info("mode: %s\n", args.mode)
    if args.mode in ["workspace", "layer"]:
        logger.info("item to query: %s", args.item)
    logger.info("GeoServer: %s", args.geoserver)
    logger.info("dry-run: %s", args.dry_run)
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


def print_report(errors):
    logger.info("\nProcessing ended, here is a summary of the collected errors:")
    for err in errors:
        logger.info("* %s", err)
    logger.info("\nend time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="""the mode to consider:
     "full" for the whole WxS server (see the "--wxs-server" option),
     "workspace" for a workspace,
     "layer" for a single layer""", choices=['full', 'workspace', 'layer'],
                        required=True)

    parser.add_argument("--item", help="""indicates the item (layer or workspace) name, see the "mode" option.
                                       The option is ignored in "full" mode.""")
    parser.add_argument("--geoserver", help="the GeoServer to use.", required=True)
    parser.add_argument("--dry-run", help="Dry-run mode", action='store_true', default=False)
    parser.add_argument("--disable-ssl-verification", help="Disable certificate verification", action="store_true")
    #parser.set_defaults(dry_run=False)

    args = parser.parse_args(sys.argv[1:])
    creds = Credentials(logger=logger)

    if args.disable_ssl_verification:
        bypassSSLVerification()

    (user, password) = creds.getFromUrl(args.geoserver)
    gscatalog = Catalog(args.geoserver + "/rest/", username=user, password=password)
    errors = []
    # Whole geoserver catalog
    if args.mode == "full":
        print_banner(args)
        # Layers
        workspaces = gscatalog.get_workspaces()
        for ws in workspaces:
            logger.debug("Inspecting workspace : %s" % ws)
            resources = gscatalog.get_resources(workspace=ws)
            for res in resources:
                try:
                    layer = gscatalog.get_layer(res.workspace.name + ":" + res.name)
                    logger.debug("Inspecting layer : %s:%s (%s)" % (res.workspace.name, res.name, layer))
                    gn_to_gs_fix(layer, res, args.dry_run, creds)
                except Inconsistency as e:
                    logger.debug("Inconsistency found : %s" % e)
                    errors.append(e)
        # Layer groups TODO: not managed yet by gsconfig
        # lgroups = gscatalog.get_layergroups()
        # for lg in lgroups:
        #     gn_to_gs_fix(lg, args.dry_run, creds)
    # Workspace
    elif args.mode == "workspace":
        if args.item is None:
            print("Missing item option")
            parser.print_help()
            sys.exit()
        print_banner(args)
        workspace = gscatalog.get_workspace(name=args.item)
        if workspace is None:
            logger.error("workspace \"%s\" not found" % args.item)
            sys.exit()
        else:
            resources = gscatalog.get_resources(workspace=workspace)
            for res in resources:
                try:
                    layer = gscatalog.get_layer(res.workspace.name + ":" + res.name)
                    gn_to_gs_fix(layer, res, args.dry_run, creds)
                except Inconsistency as e:
                    errors.append(e)
    # Single layer
    else:
        # TODO: weird ... gsconfig.get_layer(name="...") returns always a layer, even if it does not exist ...
        # better off parsing every resources available ? What if the GS has a huge catalog ?
        # loop on the Layers
        # Also, the layergroups can actually be associated to a workspace under one restriction: all
        # the composite layers should be in the same workspace as the layergroup itself.
        # The case of layergroups in a workspace is not yet addressed.
        # Anyway, gsconfig does not implement the metadata URL management on layergroups (see layergroup.py).
        print_banner(args)
        resource_found = None
        workspaces = gscatalog.get_workspaces()
        for ws in workspaces:
            resources = gscatalog.get_resources(workspace=ws)
            for res in resources:
                fullname = ws.name + ":" + res.name
                if args.item == res.name or args.item == fullname:
                    resource_found = res
                    break
            if resource_found is not None:
                break
        # Still not found ? trying on the layergroups
        # TODO: Cannot update layergroups properties
        # if resource_found is None:
        #     lgroups = gscatalog.get_layergroups()
        #     for lg in lgroups:
        #         if lg.name == args.item:
        #             resource_found = lg
        #             break
        # resource not found in the whole GeoServer
        if resource_found is None:
            logger.error("Ressource \"%s\" not found." % args.item)
            sys.exit()
        # Actually process the provided resources
        else:
            logger.debug("Resource \"%s\" found, processing ..." % resource_found.name)
            try:
                layer = gscatalog.get_layer(resource_found.workspace.name + ":" + resource_found.name)
                gn_to_gs_fix(layer, resource_found, args.dry_run, creds)
            except Inconsistency as e:
                errors.append(e)
    print_report(errors)

