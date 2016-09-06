class Valet(object):
    """
    The user interface object for this package.
    Must be able to operate in parallel.
    Created by an Abbey

    """
    def __init__(self, abbey):
        self.abbey = abbey
        self.config = abbey.config

    def print_dataset_list(self, **filters):
        entries = self.abbey.steward.list_dataset_info(**filters)
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
