import configparser

import utils

"""
Tests the default mapping ini file load.
"""

def testWorkspacesMapping():
    workspaces_mappings = utils.load_workspaces_mapping("./template/workspaces-mapping.ini.example")
    assert('test' in workspaces_mappings)
    assert('WMS' in workspaces_mappings['test']['title_wms'])