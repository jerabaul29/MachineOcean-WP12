# -*- coding: utf-8 -*-
"""Machine Ocean Tools Init

 Init File for the Machine Ocean Toolbox
"""

import logging
import os
import time

__package__    = "Machine Ocean Tools"
__author__     = "MET Norway"
__copyright__  = "Copyright 2020, MET Norway"
__license__    = ""
__version__    = ""
__url__        = "https://machineocean.met.no"
__credits__    = [
    "Jean Rabault",
    "Veronica Berglyd Olsen",
    "Martin Lilleeng Sætra",
]

# Initiating logging
logger = logging.getLogger(__name__)

# Make sure the interpreter is in UTC in all the following
os.environ["TZ"] = "UTC"
time.tzset()

# Check that basemap is present and in high enough version.
# Doing it here rather than in requirements, as basemap is not
# on Pypi any longer; see: https://matplotlib.org/basemap/users/download.html
try:
    basemap_version = None
    basemap_version_needed = "1.2.1"
    import mpl_toolkits.basemap as bm
    basemap_version = bm.__version__
    assert(basemap_version >= basemap_version_needed)
except:
    raise ImportError("Need basemap in a recent version: at least v {}, present is {}; this is not on Pypi any longer. Try with conda or ```sudo apt-get install -y python-mpltoolkits.basemap``` to install.".format(basemap_version_needed, basemap_version))

