class AbbeyError(StandardError):
    pass


class NoSuchDataset(AbbeyError):
    def __init__(self, name, version):
        message = "No record of dataset called '{}' version {}".format(name,version)
        super(NoSuchDataset, self).__init__(message)

class DatasetAlreadyExists(AbbeyError):
    def __init__(self, name, version):
        message = "Dataset called '{}' version {} seems to exist already".format(name,version)
        super(DatasetAlreadyExists, self).__init__(message)
        

class NoSuchSchema(AbbeyError):
    def __init__(self, name, version):
        message = "No record of schema called '{}' version {}".format(name,version)
        super(NoSuchSchema, self).__init__(message)

class SchemaAlreadyExists(AbbeyError):
    def __init__(self, name, version):
        message = "Schema called '{}' version {} seems to exist already".format(name,version)
        super(SchemaAlreadyExists, self).__init__(message)
  