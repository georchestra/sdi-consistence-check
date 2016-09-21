class Inconsistency:
    pass

# Scenario 1.a: GN -> GS (strict method)
class LayerNotFoundInconsistency(Inconsistency):
    """
    Class for inconsistency when a metadata contains URL to a layer which is not valid
    """
    def __init__(self):
        pass


# Scenario 1.c: GS -> GN
class MetadataInvalidInconsistency(Inconsistency):
    """
    Class which traces inconsistencies when a layer defines a Metadata URL which is
    not reachable or invalid.
    """
    def __init__(self, layerName, mdUrl):
        self.mdUrl = mdUrl
        self.layerName = layerName


    def __str__(self):
        return "Metadata %s not found or invalid for layer %s" % self.layerName, self.mdUrl


class MetadataMissingInconsistency(Inconsistency):
    """
    Class which traces inconsistencies when a layer is defined in the WMS GetCapabilities
    with no metadata URL.
    """
    def __init__(self, layerName):
        self.layerName = layerName

    def __str__(self):
        return "No metadata defined for layer %s" % self.layerName

