import os
from astropy.table import Table



class Dataset(object):
    """
    A reference to a single HDF5 file on disk.
    With a schema.
    With a UUID.
    Convert the UUID to a friendly name.
    Metadata.  In a /metadata path I think.
    Read, write operations.  Including chunks and ranges.
    Close method.

    Can validate against a schema. Or is automatically validated.  May have extra columns.
    """
    def __init__(self, path, schema, mode, size=None, metadata=None, comm=None):
        "Size can be a dictionary if there is more than one section in the schema"
        if comm is None:
            self.driver_args = {}
            self.parallel = False
        else:
            self.driver_args = {"comm":comm, "driver":"mpio"}
            self.parallel = True

        if mode == "r":
            self.file = self.validate_against_schema(path, schema)
        elif mode == "w":
            if size is None or metadata is None:
                raise RuntimeError("Must specify metadata to create new dataset")
            self.file = self.create_with_schema(path, schema, size, metadata)
        else:
            raise ValueError("Unknown dataset mode {}".format(mode))
        self.mode = mode
        self.path=path
        self.schema=schema

    def __repr__(self):
        return '<Dataset of type "{}" v{} mode "{}">'.format(self.schema.name, self.schema.version, self.mode, self.path)
    

    def validate_against_schema(self, path, schema):
        import h5py
        f = h5py.File(path, mode="r", **self.driver_args)
        schema.validate_dataset(f)
        return f

    def create_with_schema(self, path, schema, size, metadata):
        import h5py
        if os.path.exists(path):
            raise ValueError("File opened for reading already exists: {}".format(path))
        schema.validate_metadata(metadata)
        
        f = h5py.File(path, mode="w", **self.driver_args)
        schema.create_structure(f, size, metadata)
        return f

    def find_section(self, section_name):
        if section_name is None:
            keys = self.file.keys()
            if len(keys)==1:
                section_name = keys[0]
            else:
                raise RuntimeError("Multiple section names in data file - please set section_name= one of : {}".format(', '.join(keys)))
        section = self.file[section_name]
        return section

    def read(self, section_name=None, range=None, columns=None):
        if self.mode != "r":
            raise RuntimeError("Tried to read data from a file opened in mode: {}".format(self.mode))

        section = self.find_section(section_name)
        # Data files can have a number of sections.  
        # If there is only one then it is unambiguous to just read the only one.
        # otherwise it is an error not to specify.

        # We might wish to read the entire catalog or just a chunk of it
        if columns is None:
            columns = section.keys()

        if range is None:
            data = [section[column][:] for column in columns]
        else:
            start,end = range
            data = [section[column][start:end] for column in columns]

        table = Table(names=columns, data=data)
        return table


    def write(self, chunk, section_name=None, range=None, columns=None):
        if self.mode != "w":
            raise RuntimeError("Tried to write data to a file opened in mode: {}".format(self.mode))

        # chunk must be a list of same length as the number of columns
        # each 
        section = self.find_section(section_name)
        if columns is None:
            columns = section.keys()

        assert len(columns) == len(chunk)

        if range is None:
            for column, chunk_column in zip(columns, chunk):
                section[column][:] = chunk_column
        else:
            start,end = range
            for column, chunk_column in zip(columns, chunk):
                section[column][start:end] = chunk_column

    def close(self):
        self.file.close()

