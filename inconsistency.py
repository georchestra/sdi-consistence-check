class Inconsistency(Exception):
    def fix(self):
        raise NotImplementedError('Not implemented')


# Scenario 0: unable to parse the GetCapabilities
class UnparseableGetCapabilitiesInconsistency(Inconsistency):
    """
    Class for inconsistency when the remote GetCapabilities fails to parse.
    """
    def __init__(self, owsUrl, message):
        self.owsUrl = owsUrl
        self.msg = message

    def __str__(self):
        return "The OWS GetCapabilities is unparseable at %s: %s" % (self.owsUrl, self.msg)


# Scenario 1.a: GN -> GS (strict method)
class GnToGsLayerNotFoundInconsistency(Inconsistency):
    """
    Class for inconsistency when a metadata contains URL to a layer which is not valid
    """
    def __init__(self, layer_url, layer_name, md_uuid=None, msg=None):
        self.layer_url = layer_url
        self.layer_name = layer_name
        self.md_uuid = md_uuid
        self.msg = msg

    def set_md_uuid(self, uuid):
        self.md_uuid = uuid

    def __str__(self):
        return "Metadata %s references a layer : %s on %s that does not exist (%s)" \
               % (self.md_uuid, self.layer_name, self.layer_url, self.msg)

class GnToGsOtherError(Inconsistency):
    """
    Class for errors in underlying libraries (owslib), not directly
    managed by this project
    """
    def __init__(self, layer_url, layer_name, exc):
        self.layer_url = layer_url
        self.layer_name = layer_name
        self.exc = exc

    def set_md_uuid(self, uuid):
        self.md_uuid = uuid

    def __str__(self):
        return "%s: %s" % (self.exc.__class__.__name__, str(self.exc))


class GnToGsInvalidCapabilitiesUrl(Inconsistency):
    """
    Class for inconsistency when a metadata contains URL to a layer which is not valid
    """
    def __init__(self, layer_url, layer_name, is_wms, md_uuid=None, msg=None):
        self.layer_url = layer_url
        self.layer_name = layer_name
        self.md_uuid = md_uuid
        self.msg = msg
        self.is_wms = is_wms

    def set_md_uuid(self, uuid):
        self.md_uuid = uuid

    def __str__(self):
        return "Metadata %s references a layer : %s on %s which is not a valid %s GetCapabilities URL" \
               % (self.md_uuid, self.layer_name, self.layer_url,
                  "WMS" if self.is_wms else "WFS")

class GnToGsNoOGCWmsDefined(Inconsistency):
    """
    Class used to describe a data metadata which misses a URL with OGC:WMS protocol
    """
    def __init__(self, md_uuid):
        self.md_uuid = md_uuid

    def __str__(self):
        return "Metadata %s does not reference any url with protocol OGC:WMS" \
            % self.md_uuid


class GnToGsNoOGCWfsDefined(Inconsistency):
    """
    Class used to describe a data metadata which misses a URL with OGC:WFS protocol
    """
    def __init__(self, md_uuid):
        self.md_uuid = md_uuid

    def __str__(self):
        return "Metadata %s does not reference any url with protocol OGC:WFS" \
               % self.md_uuid

class GnToGsNoGetCapabilitiesUrl(Inconsistency):
    """
    Class used to describe when a service metadata does not reference a GetCapabilities
    service URL.
    """
    def __init__(self, servicemd_uuid, datamd_uuid):
        self.servicemd_uuid = servicemd_uuid
        self.datamd_uuid = datamd_uuid

    def __str__(self):
        return "Service Metadata \"%s\" linked to the data metadata \"%s\" has no " \
            "GetCapabilities URL defined" % (self.servicemd_uuid, self.datamd_uuid)

# Scenario 1.c: GS -> GN
class GsToGnMetadataInvalidInconsistency(Inconsistency):
    """
    Class which traces inconsistencies when a layer defines a Metadata URL which is
    not reachable or invalid.
    """
    def __init__(self, md_url, message, layer_name=None):
        self.md_url = md_url
        self.layer_name = layer_name
        self.message = message

    def __str__(self):
        return "Metadata %s not found or invalid for layer '%s': %s" % (self.md_url, self.layer_name, self.message)


class GsMetadataMissingInconsistency(Inconsistency):
    """
    Class which traces inconsistencies when a layer is defined in the WMS GetCapabilities
    with no metadata URL.
    Note: this class is used in both scenarii (2 and 3), hence the name.
    """
    def __init__(self, layer_name, layer_idx=None):
        self.layer_name = layer_name
        self.layer_index = layer_idx

    def __str__(self):
        return "No metadata defined for layer %s" % self.layer_name


# Scenario 3: Inconsistency to keep track of errors when trying to insert a metadata
class GsToGnUnableToCreateServiceMetadataInconsistency(Inconsistency):
    """
    Class which gathers errors when trying to CSW-T insert a service metadata.
    """
    def __init__(self, workspace, catalogue_url, caused_by):
        self.workspace = workspace
        self.catalogue_url = catalogue_url
        self.caused_by = caused_by

    def __str__(self):
        return "Unable to save the service metadata for workspace \"%s\" into %s: %s" % (self.workspace,
            self.catalogue_url, self.caused_by)

class GsToGnUnableToUpdateServiceMetadataInconsistency(Inconsistency):
    """
    Class which gathers errors when trying to CSW-T update a service metadata.
    """
    def __init__(self, workspace, mds_uuid, catalogue_url, caused_by):
        self.workspace = workspace
        self.catalogue_url = catalogue_url
        self.caused_by = caused_by
        self.mds_uuid = mds_uuid

    def __str__(self):
        return "Unable to update the service metadata (uuid: %s) for workspace \"%s\" into %s: %s" % (self.mds_uuid,
            self.workspace, self.catalogue_url, self.caused_by)