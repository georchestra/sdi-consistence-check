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
# * Attribution (récupérer le useLimitation, et regexp sur "(.*)")
#   md.identificationinfo[0].uselimitation[0]
#

from inconsistency import GsToGnMetadataMissingInconsistency


class GeonetworkToGeoserverUpdater:

    def __init__(self, gn_url, gs_url, creds=Credentials(), dryrun=False):
        self.credentials = creds
        # TODO: Do we have a better way to guess the GN url ?
        # This will be needed to construct MD xml / HTML urls, and is GeoNetwork(geOrchestra)-proprietary ...
        self.gn_url = gn_url
        self.gs_url = gs_url
        self.cswq = CSWQuerier(gn_url + "/srv/eng/csw")
        self.gscatalog = Catalog(gs_url + "/rest/")
        self.dryrun = dryrun

    def _get_wxs_online(self, md):
        return [ o for o in md.distribution.online if o.protocol.startswith("OGC:WMS") \
                 and o.url.startswith(self.gs_url) ]

    def _update_resource(self, res, title, abstract, md_url_xml, md_url_html):
        # Updates the MD title (if not present or if the MD title is bigger)
        upd_title = False
        upd_abstract = False
        if res.title is None or res.title < title:
            res.title = title
            upd_title = True
        # Same algo for the abstract
        if res.abstract is None or res.abstract < abstract:
            res.abstract = abstract
            upd_abstract = True
        # Check that MD Urls are present
        has_md_html = False
        has_md_xml = False
        mdlinks = res.metadata_links
        if mdlinks is not None:
            for lnk in res.metadata_links:
                if lnk[0] == "application/xml":
                    has_md_xml = True
                if lnk[0] == "text/html":
                    has_md_html = True
        else:
            mdlinks = []
        if not has_md_xml:
            mdlinks.append(("text/xml", "19139", md_url_xml))
        if not has_md_html:
            mdlinks.append(("text/html", "19139", md_url_html))
        if not self.dryrun:
            res.metadata_links = mdlinks
            catalog = res.catalog
            catalog.save(res)
            catalog.reload()
            print("\"%s\": layer info updated" % res.title)
        else:
            print("dry-run mode: not updating the resource for layer \"%s\"" % res.title)
            if upd_title:
                print("\t- the title of the resource should have been updated")
            if upd_abstract:
                print("\t- the abstract of the resource should have been updated")
            if not has_md_html:
                print("\t- an HTML metadata url should have been added")
            if not has_md_xml:
                print("\t- a XML metadata url should have been added")

    def fix(self):
        mds = self.cswq.get_data_mds()
        for uuid, md in mds.items():
            title = md.identificationinfo[0].title
            abstract = md.identificationinfo[0].abstract
            # These need to be calculated
            md_url_xml = "%s/srv/eng/xml.metadata.get?uuid=%s" % (self.gn_url, md.identifier)
            # MD URL for HTML output: GN2 vs GN3 ?
            # GN3: gn_url + "#/<identifier>"
            # GN2: gn_url + "apps/georchestra?uuid=<identifier>"
            # Both seem to work with just gn_url + ?uuid=identifier anyway
            md_url_html = "%s/?uuid=%s" % (self.gn_url, md.identifier)
            online_res = self._get_wxs_online(md)
            for ol in online_res:
                layer = self.gscatalog.get_layer(ol.name)
                # if layer is None: new inconsistency (layer not found in GS)
                # TODO: next challenge is now to find the featuretype/coveragestore/wmslayer associated with this layer
                #
                # workspaces -> stores (datastore,coveragestore,wmsstore) -> layer
                # each layer can have some extra info into a file named (featuretype.xml, coverage.xml, wmslayer.xml)
                # depending on the store's type the layer belongs to
                #
                # The way the information is accessed through the REST interface is unclear ... Some endpoints do not
                # seem to work as expected, gsconfig does not provide convenient way to fetch the information.
                #
                # gscatalog.get_layers() returns every layers of the catalog (at least visible by the current user)
                # but there is no way to get the store back.
                #
                # Anyway, there is still the possibility to call gscatalog.get_resource(), but how to deal with
                # layers named the same way but belonging to different workspaces ?
                #
                # Other GS-related remarks:
                # - Layer groups are not in a specific workspace, and gsconfig does not allow the modification
                # of the metadataUrls.
                #
                # Note: to update a resource with gsconfig, see:
                # https://github.com/boundlessgeo/gsconfig/blob/master/test/catalogtests.py#L193-L208

                # Looks up the associated resource
                res = None
                try:
                    (workspace_name, layer_name) = layer.name.split(":", 1)
                    res = self.gscatalog.get_resource(layer_name,
                                                      workspace=self.gscatalog.get_workspace(workspace_name))
                except ValueError:
                    # if no ":" in the name, then we probably have a layergroup
                    # res = gscatalog.get_layergroup(layer.name)
                    # LayerGroup.title does not exist ...
                    print("%s: Layer group is not editable yet" % layer.name)
                    continue
                if res is not None:
                    self._update_resource(res, title, abstract, md_url_xml, md_url_html)
                else:
                    print("resource not found for layer %s" % layer.name)

# Logging configuration
logger = logging.getLogger("GnToGsUpdater")
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)


def update_resource(resource, title, abstract, md_url_html, dry_run):
    """
    Updates a Geoserver resource
    :param resource: a gsconfig resource
    :param title: the title to set
    :param abstract: the abstract to set
    :param md_url_html: the metadata url for the HTML version
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
        logger.info("\"%s\": layer info updated" % res.title)
    else:
        logger.info("dry-run mode: not updating the resource for layer \"%s\"" % res.title)
        if upd_title:
            logger.info("\t- the title of the resource should have been updated")
        if upd_abstract:
            logger.info("\t- the abstract of the resource should have been updated")
        if not has_md_html:
            logger.info("\t- an HTML metadata URL should have been added")

def find_metadata(resource):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :return: the parsed metadata.
    """
    if resource.metadata_links is None:
        raise GsToGnMetadataMissingInconsistency(resource.workspace + ":" + resource.name)
    for mime_type, format, url in resource.metadata_links:
        if mime_type == "text/xml" and format == "ISO19115:2003":
            with urlopen(url) as fhandle:
                return MD_Metadata(etree.parse(fhandle))
    raise GsToGnMetadataMissingInconsistency(resource.workspace + ":" + resource.name)


def gn_to_gs_fix(resource, dry_run):
    print(resource.name)
    md = find_metadata(resource)
    print(md.identifier)

def print_banner(args):
    logger.info("\nGeoNetwork To Geoserver Updater\n\n")
    logger.info("mode: %s\n", args.mode)
    if args.mode in ["workspace", "layer"]:
        logger.info("item to query: %s", args.item)
    logger.info("GeoServer to query: %s", args.geoserver)
    logger.info("dry-run: %s", args.dry_run)
    logger.info("\nstart time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))
    logger.info("\n\n")


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
                gn_to_gs_fix(res, args.dry_run)
        # Layer groups
        lgroups = gscatalog.get_layergroups()
        for lg in lgroups:
            gn_to_gs_fix(lg, args.dry_run)
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
                gn_to_gs_fix(res, args.dry_run)
    # Single layer
    else:
        # TODO: weird ... gsconfig.get_layer(name="...") returns always a layer, even if it does not exist ...
        # better off parsing every resources available ? What if the GS has a huge catalog ?
        # loop on the Layers
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
        if resource_found is None:
            lgroups = gscatalog.get_layergroups()
            for lg in lgroups:
                if lg.name == args.item:
                    resource_found = lg
                    break
        # resource not found in the whole GeoServer
        if resource_found is None:
            logger.error("Ressource \"%s\" not found." % args.item)
            sys.exit()
        # Actually process the provided resources
        else:
            logger.error("Resource \"%s\" found, processing ..." % resource_found.name)
            gn_to_gs_fix(resource_found, args.dry_run)


    #gs2gnupd = GeonetworkToGeoserverUpdater("http://localhost:8080/geonetwork",
    #                                        "http://localhost:8080/geoserver", dryrun=DRY_RUN)
    #gs2gnupd.fix()




