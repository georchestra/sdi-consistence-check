import argparse
import logging

import sys
from time import strftime, localtime
import xml.etree.ElementTree as etree
from urllib.request import urlopen

from geoserver.catalog import Catalog
from owslib.iso import MD_Metadata

from credentials import Credentials
from cswquerier import CSWQuerier

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

from inconsistency import GsToGnMetadataMissingInconsistency, Inconsistency

# Logging configuration
logger = logging.getLogger("GnToGsUpdater")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


def update_resource(resource, title, abstract, md_url_html, attribution, dry_run):
    """
    Updates a Geoserver resource
    :param resource: a gsconfig resource
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
    if resource.title is None or resource.title < title:
        resource.title = title
        upd_title = True
    # Same algo for the abstract
    if resource.abstract is None or resource.abstract < abstract:
        resource.abstract = abstract
        upd_abstract = True
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
        resource.metadata_links = mdlinks
        catalog = res.catalog
        catalog.save(res)
        catalog.reload()
        logger.info("\"%s\": layer info updated\n" % res.title)
    else:
        logger.info("dry-run mode: not updating the resource for layer \"%s\"" % res.title)
        if upd_title:
            logger.info("\t- the title of the resource should have been updated")
        if upd_abstract:
            logger.info("\t- the abstract of the resource should have been updated")
        if not has_md_html:
            logger.info("\t- an HTML metadata URL should have been added")
        logger.info("\n")

def find_metadata(resource):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :return: the parsed metadata.
    """
    if resource.metadata_links is None:
        raise GsToGnMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)
    for mime_type, format, url in resource.metadata_links:
        if mime_type == "text/xml" and format == "ISO19115:2003":
            with urlopen(url) as fhandle:
                return MD_Metadata(etree.parse(fhandle))
    raise GsToGnMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)


def gn_to_gs_fix(resource, dry_run):
    md = find_metadata(resource)
    md_title = md.identificationinfo[0].title
    md_abstract = md.identificationinfo[0].abstract
    # TODO: what if not GeoNetwork ?
    md_url_html = md.identifier
    md_attribution = "geOrchestra corporation, a holding by camptocamp"
    update_resource(resource, md_title, md_abstract, md_url_html, md_attribution, dry_run)

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
    logger.info("Processing ended, here is a summary of the collected errors:")
    for err in errors:
        logger.info("* %s", err)
    logger.info("\nend time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="""the mode to consider:
     "full" for the whole WxS server (see the "--wxs-server" option),
     "workspace" for a workspace,
     "layer" for a single layer""", choices=['full', 'workspace', 'layer'])

    parser.add_argument("--item", help="""indicates the item (layer or workspace) name, see the "mode" option.
                                       The option is ignored in "full" mode.""")
    parser.add_argument("--geoserver", help="the GeoServer to use.")
    parser.add_argument("--dry-run", help="Dry-run mode, default true", choices=[True, False], default=True)

    args = parser.parse_args(sys.argv[1:])

    if (args.mode is None or args.mode not in ["full", "workspace", "layer"]
        or args.geoserver is None):
        parser.print_help()
        sys.exit()

    gscatalog = Catalog(args.geoserver + "/rest/")
    errors = []
    # Whole geoserver catalog
    if args.mode == "full":
        print_banner(args)
        # Layers
        workspaces = gscatalog.get_workspaces()
        for ws in workspaces:
            resources = gscatalog.get_resources(workspace=ws)
            for res in resources:
                try:
                    gn_to_gs_fix(res, args.dry_run)
                except Inconsistency as e:
                    errors.append(e)
        # Layer groups TODO: not managed yet by gsconfig
        # lgroups = gscatalog.get_layergroups()
        # for lg in lgroups:
        #     gn_to_gs_fix(lg, args.dry_run)
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
                    gn_to_gs_fix(res, args.dry_run)
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
                gn_to_gs_fix(resource_found, args.dry_run)
            except Inconsistency as e:
                errors.append(e)
    print_report(errors)

