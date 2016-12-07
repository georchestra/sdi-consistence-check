class Inconsistency(BaseException):
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
        return "Metadata %s not found or invalid for layer %s: %s" % (self.layer_name, self.md_url, self.message)


class GsToGnMetadataMissingInconsistency(Inconsistency):
    """
    Class which traces inconsistencies when a layer is defined in the WMS GetCapabilities
    with no metadata URL.
    """
    def __init__(self, layer_name):
        self.layer_name = layer_name

    def __str__(self):
        return "No metadata defined for layer %s" % self.layer_name

