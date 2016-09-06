import os
from . import steward, valet
from . import config

class Valet(object):
    """
    """
    def __init__(self, config_path, mpi=False):
        self.config_path = config_path
        self.config = config.Config(config_path)
        self.steward = steward.Steward(self.config)

    def print_dataset_list(self, **filters):
        entries = self.steward.list_dataset_info(**filters)
        for entry in entries:
            print entry

    def open_dataset(self, name, schema, version=None):
        info = self.steward.get_dataset_info(name,version)
        dataset = self.steward.open_dataset(info, schema)
        return dataset

    def create_dataset(self, name, version, schema, size, metadata):
        return self.steward.create_dataset(name, version, schema, size, metadata)

    def get_schema(self, schema_name, schema_version=None):
        return self.steward.get_schema(schema_name, version=schema_version)

    @classmethod
    def create_repository(self, repository_path):
        "Not used in user code: run once on a machine to create a new repo"
        pass





