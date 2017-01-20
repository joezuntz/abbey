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
        self._validate_dataset_metadata(dataset)

    def _validate_dataset_metadata(self, dataset):
        try:
            metadata = dataset['metadata'].attrs
        except KeyError:
            raise ValidationError("Metadata section not found in file")

        #Validate the name and version of the schema.
        self._validate_value(metadata, "schema_name", self.name)
        self._validate_value(metadata, "schema_version", self.version)

        #Validate the user metadata
        self.validate_metadata(metadata)

    def validate_metadata(self, metadata):
        self._validate_group(metadata, self.required_metadata)

    def _validate_data(self, dataset):
        #Validate the data columns themselves
        cols = [("{}/{}".format(c[0],c[1]), c[2]) for c in self.columns]
        self._validate_group(dataset, cols)

    def _validate_value(self, metadata, metadata_name, desired_value):
        try:
            found_value = metadata[metadata_name]
            assert found_value == desired_value
        except KeyError:
            raise ValidationError("{} not found in file metadata".format(metadata_name))
        except AssertionError:
            raise ValidationError("{} wrong in file metadata: {} not {}".format(metadata_name, found_value, desired_value))

    def _validate_group(self, group, required):
        for name,dtype in required:
            try:
                val = group[name]
            except KeyError:
                raise ValidationError("Required Item '{}' not found.".format(name))
            val_dtype = np.array(val).dtype
            if val_dtype != dtype:
                raise ValidationError("Required Item '{}' has wrong type: {} instead of {}".format(name, val_dtype, dtype))

    def create_structure(self, f, size, metadata):
        # add metadata
        g = f.create_group("/metadata")
        for key, value in metadata.items():
            g.attrs[key] = value
        g.attrs['schema_name'] = self.name
        g.attrs['schema_version'] = self.version

        # add columns
        sections = set()
        for (section, name, dtype) in self.columns:
            f.create_dataset("{}/{}".format(section,name), shape=(size,), maxshape=(None,), dtype=dtype, chunks=True)
        



