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

class Valet(object):
    """
    """
    def __init__(self, config_path, mpi=False):
        self.config_path = config_path
        self.config = config.Config(config_path)
        self.steward = steward.Steward(self.config)
        pass

    def print_dataset_list(self, **filters):
        entries = self.steward.list_dataset_info(**filters)
        for entry in entries:
            print entry

    def find_dataset(self, name, schema, version=None):
        # returns a Dataset object corresponding to an existing file, checking that it matches
        # the named schema
        # If version==None just find the latest version.
        pass

    def create_dataset(self, name, schema, must_be_first):
        # creates a new data set with the given schema and name.
        # careful with parallelization here!
        pass

    def get_schema(self, schema_name):
        pass

    @property
    def repo_path(self):
        return self.config.path

    def get_valet(self):
        return self.valet

    @classmethod
    def create_repository(self, repository_path):
        "Not used in user code: run once on a machine to create a new repo"
        # checks if directory exists and is not empty
        # creates abbey.cfg with defaults
        # creates directory structure for schemas and datasets
        pass





