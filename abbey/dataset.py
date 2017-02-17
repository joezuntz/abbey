import os



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
        import h5py
        if comm is None:
            self.driver_args = {}
            self.parallel = False
        else:
            self.driver_args = {"comm":comm, "driver":"mpio"}
            self.parallel = True

        self.mode = mode
        self.path=path
        self.schema=schema

        if mode == "r":
            self.file = h5py.File(path, mode="r", **self.driver_args)
            self.metadata = self.file['metadata'].attrs
            schema.validate_dataset(self)
        elif mode == "w":
            if size is None or metadata is None:
                raise RuntimeError("Must specify metadata to create new dataset")
            self.create_with_schema(path, schema, size, metadata)
        else:
            raise ValueError("Unknown dataset mode {}".format(mode))

    def __repr__(self):
        return '<Dataset of type "{}" v{} mode "{}">'.format(self.schema.name, self.schema.version, self.mode, self.path)
    
    def has_column(self, name, section_name=None):
        try:
            section = self.find_section(section_name)
        except KeyError:
            return False
        return name in section

    def column_type(self, name, section_name):
        try:
            section = self.find_section(section_name)
        except KeyError:
            return False
        col = section[name]
        return col.dtype


    def create_with_schema(self, path, schema, size, metadata):
        import h5py
        if os.path.exists(path):
            raise ValueError("File opened for reading already exists: {}".format(path))
        schema.validate_metadata(metadata)
        self.file = h5py.File(path, mode="w", **self.driver_args)
        self.file.create_group("metadata")
        self.metadata = self.file['metadata'].attrs

        schema.create_structure(self, size, metadata)


    def resize(self, size, section_name=None):
        section = self.find_section(section_name)
        for col in section.values():
            col.resize(size)

    def add_columns(self, names, dtypes, section_name=None, size=None):
        try:
            section = self.find_section(section_name)
        except KeyError:
            if section_name is None:
                raise ValueError("Must specify section_name and size to create completely new sections")
            self.file.create_group(section_name)
            section = self.find_section(section_name)
        existing_keys = section.keys()
        if existing_keys:
            size = existing_keys[0].size
        else:
            if size is None:
                raise ValueError("Must specify a size to create a new section.")

        for name,dtype in zip(names,dtypes):
            if name not in existing_keys:
                section.create_dataset(name, shape=(size,), dtype=dtype, chunks=True, maxshape=(None,))

    def keys(self, section_name=None):
        section = self.find_section(section_name)
        return section.keys()

    def find_section(self, section_name):
        if section_name is None:
            keys = self.file.keys()
            if len(keys)==1:
                section_name = keys[0]
            elif len(keys)==0:
                raise KeyError("No sections in this file yet.")
            else:
                raise RuntimeError("Multiple section names in data file - please set section_name= one of : {}".format(', '.join(keys)))
        section = self.file[section_name]
        return section

    def read(self, section_name=None, range=None, columns=None, table_maker=None):
        if self.mode != "r":
            raise RuntimeError("Tried to read data from a file opened in mode: {}".format(self.mode))

        if table_maker is None:
            import astropy.table
            table_maker = astropy.table.Table



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

        table = table_maker(names=columns, data=data)
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

