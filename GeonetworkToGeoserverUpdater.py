from geoserver.catalog import Catalog

from credentials import Credentials
from cswquerier import CSWQuerier

# Scénario 2 Read-Write GN -> GS
#
# Le document de spécification rédigé par C2C prévoit une interrogation préalable du GeoServer. A la relecture
# Je ne comprends pas bien l'intéret, je pense qu'on peut récupérer directement les MDDs depuis GN, les parser,
# et s'assurer coté GS que "tout est OK".
# Par ailleurs, la demande provenant de Rennes-Métropole prévoit de s'assurer de la cohérence des champs suivants:
#
# * Titre
# * nom de la couche (c.f. MDs dans geOrchestra, geopicardie est un bon élève)
# * résumé
# * url vers la MD en HTML
# * url vers la MD en XML
# * Attribution (pas clair dans les spécifications ? Point de contact ? de quel role ?)
#
# Le résultat prévoit en outre:
#
# * un test de la couche (savoir si d'unte part elle existe dans GS, d'autre part si elle est visible sans erreur
# * Information non présente dans la MD mais dans GS (/!\ écrasement si plus d'info d'un coté ou de l'autre ?)
#


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
            mdlinks.append(("application/xml", "19139", md_url_xml))
        if not has_md_html:
            mdlinks.append(("text/html", "19139", md_url_html))
        # TODO: '19139' above is not yet managed by the gsconfig (gsconfig-py3) library,
        # see https://github.com/boundlessgeo/gsconfig/pull/166
        # We might need to use ISO19115:2003 instead (this is what géopicardie does anyway)
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


DRY_RUN = False

if __name__ == "__main__":
    gs2gnupd = GeonetworkToGeoserverUpdater("http://localhost:8080/geonetwork",
                                            "http://localhost:8080/geoserver", dryrun=DRY_RUN)
    gs2gnupd.fix()




