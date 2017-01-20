import os
from . import steward
from . import config
from sqlalchemy.exc import IntegrityError

class Valet(object):
    """
    """
    def __init__(self, config_path, mpi_comm=None):
        if isinstance(config_path, config.Config):
            self.config = config_path
        else:
            self.config = config.Config(config_path)
        self.steward = steward.Steward(self.config, mpi_comm)

    def print_dataset_list(self, **filters):
        entries = self.steward.list_dataset_info(**filters)
        for entry in entries:
            print "    {} v{}  ({} v{})".format(entry.name,entry.version,entry.schema_name,entry.schema_version)

    def print_schema_list(self, **filters):
        entries = self.steward.list_schema_info(**filters)
        for entry in entries:
            print "    {} v{}".format(entry.name,entry.version)

    def print_all(self):
        print
        print "Schemas:"
        self.print_schema_list()
        print
        print "Data sets:"
        self.print_dataset_list()
        print

    def delete_dataset(self, name, version):
        pass


    def open_dataset(self, name, schema, version=None):
        """
        Open a dataset with the specified name and schema
        """
        info = self.steward.get_dataset_info(name,version)
        if info is None:
            if version is None:
                raise ValueError('Dataset "{}" not found (any version)'.format(name))
            else:
                raise ValueError('Dataset "{}" version {} not found'.format(name, version))
        dataset = self.steward.open_dataset(info, schema)
        return dataset

    def create_dataset(self, name, version, schema, size, metadata):
        try:
            return self.steward.create_dataset(name, version, schema, size, metadata)
        except IntegrityError:
            raise ValueError('There is already a dataset "{}" version {}'.format(name, version))

    def get_schema(self, schema_name, schema_version=None):
        return self.steward.get_schema(schema_name, version=schema_version)

    def create_schema(self, name, version, columns, required_metadata):
        self.steward.create_schema(name, version, columns, required_metadata)

    @classmethod
    def create_repository(cls, config):
        "Not usually used in user code: run once on a machine to create a new repo"
        os.mkdir(config.path)
        os.mkdir(os.path.join(config.path,"data"))
        open(os.path.join(config.path, "repo.db"), 'a').close()
        return cls(config)


