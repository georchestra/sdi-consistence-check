
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
from geoserver.catalog import Catalog

from cswquerier import CSWQuerier
from inconsistency import GsToGnMetadataMissingInconsistency

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

if __name__ == "__main__":
    gscatalog = Catalog(GS_URL + "/rest/")
    ws = gscatalog.get_workspace(WS_NAME)
    res = gscatalog.get_resources(workspace=ws)
    cswq = CSWQuerier(GN_URL + "/srv/eng/csw")
    services_md = init_mdd_mds_mapping(cswq)

    mddsUrl = []
    inconsistencies = []
    for r in res:
        if r.metadata_links is not None:
            curmds = [ mdlink[2] for mdlink in r.metadata_links if mdlink[0] == 'application/xml'
                   and  mdlink[2].startswith(GN_URL) ]
            mddsUrl = mddsUrl + curmds
        if len(mddsUrl) == 0:
            inconsistencies.append(GsToGnMetadataMissingInconsistency(WS_NAME + ":" + r.name))

    # Si la couche n'a pas de MDD, remonter la couche en erreur
    # Afficher les erreurs en fin de batch (ou pendant le processus ?)
    for it in inconsistencies:
        print(it)

