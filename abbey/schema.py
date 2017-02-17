import collections
import numpy as np

class ValidationError(Exception):
    pass

class Schema(object):
    """A schema defining the column names and data types for a single hdf5 file.
    The column names may be in several sub sections
    For example section "shape_catalog"
        ("shape_cat", "coadd_object_id", "int64"),
        ("shape_cat", "e1", "float64"),
        ("shape_cat", "e2", "float64"),
        ("sky_map", "pixel", "int64"),
        ("sky_map", "value", "int64"),

    """
    def __init__(self, name, version, columns, required_metadata):
        self.columns = columns  #list of (section, name, dtype)
        self.version = version  #integer
        self.name = name
        self.required_metadata = required_metadata  #list of (name, dtype)

    def __eq__(self, other):
        return (self.name==other.name) and (self.version==other.version)

    def __ne__(self, other):
        return not self==other

    def __repr__(self):
        return '<Schema "{}" v{}>'.format(self.name,self.version)

    def validate_dataset(self, dataset):
        self._validate_data(dataset)
        self.validate_metadata(dataset.metadata)

    def validate_metadata(self, metadata):
        for name,dtype in self.required_metadata:
            if name not in metadata:
                raise ValidationError("Missing metadata: {}".format(name))
            value = np.array(metadata[name])
            dt = value.dtype
            if not dt==dtype:
                raise ValidationError("Metadata: {} has wrong type ({} not {})".format(name, dt, dtype))

    def _validate_data(self, dataset):
        #Validate the data columns themselves
        for (section, name, dtype) in self.columns:
            if not dataset.has_column(name, section):
                raise ValidationError("Missing column: {} in section {}".format(name,section))
            actual_dtype = dataset.column_type(name,section)
            if not dtype == actual_dtype:
                raise ValidationError("olumn: {} in section {} has wrong type ({} not {})".format(name,section,actual_dtype,dtype))

            #Also validate type

    def create_structure(self, dataset, size, metadata):
        # add metadata
        for key, value in metadata.items():
            dataset.metadata[key] = value
        dataset.metadata['schema_name'] = self.name
        dataset.metadata['schema_version'] = self.version

        cols_by_section = collections.defaultdict(list)
        for (section, name, dtype) in self.columns:
            cols_by_section[section].append((name,dtype))

        for section_name, cols in cols_by_section.items():
            names = [col[0] for col in cols]
            dtypes =[col[1] for col in cols]
            dataset.add_columns(names, dtypes, section_name, size)
