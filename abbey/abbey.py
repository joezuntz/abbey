import os
from . import steward, valet
from . import config
#steward.py
# Steward

# - has a data store, which is probably just a database with some file locations and metadata
# - has a collection of schemas
# - can check files still exist
# - can talk to other stewards? or a central server?
# - can create new data references
# - never looks into the files itself? or maybe just metadata?

class Abbey(object):
    """
    The master object, initialized with a directory containing all the information we need
    to track.

    Responsible only for initializing the other objects.

    abbey.steward
    abbet.valet
    """
    def __init__(self, config_path, mpi=False):
        self.config_path = config_path
        self.config = config.Config(config_path)
        self.steward = steward.Steward(self.config)
        self.valet = valet.Valet(self)
        # read configuration file in that path/abbey.cfg
        #
        # important configuration information:
        # paths?
        # anything?
        # maybe just find abbey.sqlite3 there
        pass

    @property
    def repo_path(self):
        return self.config.path

    def get_valet(self):
        return self.valet

    @classmethod
    def create(self, repository_path):
        "Not used in user code: run once on a machine to create a new repo"
        # checks if directory exists and is not empty
        # creates abbey.cfg with defaults
        # creates directory structure for schemas and datasets
        pass





