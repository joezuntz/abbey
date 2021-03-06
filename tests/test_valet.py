from abbey import Dataset, Schema, Valet
from abbey.errors import *
import os
import numpy as np
from test_utils import temporary_directory, schema_ingredients, create_test_repo
from nose.tools import assert_raises



def test_repo_create():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)


def test_valet_create_schema():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)


def test_valet_create_schema():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)


def test_valet_create_schema_versions():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)
        columns.append(("shape_cat", "new_param", "float64"),)
        valet.create_schema("cool_schema", 2, columns, required_metadata)
        # Should get the newest schema
        schema = valet.get_schema("cool_schema")
        assert schema.version == 2


def test_valet_create_dataset():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)
        # Should get the newest schema
        schema = valet.get_schema("cool_schema")

        dataset = valet.create_dataset("cool_data", 1, schema, 100, example_metadata)



def test_valet_write_dataset():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)
        # Should get the newest schema
        schema = valet.get_schema("cool_schema")

        dataset = valet.create_dataset("cool_data", 1, schema, 100, example_metadata)
        data = [np.arange(100, dtype=np.int64), (np.arange(100., dtype=np.float64)*2),(np.arange(100., dtype=np.float64)*3)]
        dataset.write(data, section_name="shape_cat")


def test_valet_read_write_dataset():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)
        # Should get the newest schema
        schema = valet.get_schema("cool_schema")

        dataset = valet.create_dataset("cool_data", 1, schema, 100, example_metadata)
        data = [np.arange(100, dtype=np.int64), (np.arange(100., dtype=np.float64)*2),(np.arange(100., dtype=np.float64)*3)]
        dataset.write(data, section_name="shape_cat")
        dataset.close()

        dataset = valet.open_dataset("cool_data", schema)

        data = dataset.read(section_name="shape_cat")
        assert (data['e1']==np.arange(100., dtype=np.float64)*2).all()
        assert (data['e2']==np.arange(100., dtype=np.float64)*3).all()

def test_valet_double_schema():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)

        #Creating a new schema should fail
        assert_raises(SchemaAlreadyExists,
                valet.create_schema,"cool_schema", 1, columns, required_metadata
            )
        schema = valet.get_schema("cool_schema")

def test_valet_double_dataset():
    with temporary_directory() as tmpdir:
        valet = create_test_repo(tmpdir)
        columns, required_metadata, example_metadata = schema_ingredients()
        valet.create_schema("cool_schema", 1, columns, required_metadata)
        # Should get the newest schema
        schema = valet.get_schema("cool_schema")

        dataset = valet.create_dataset("cool_data", 1, schema, 100, example_metadata)

        assert_raises(DatasetAlreadyExists,
                valet.create_dataset,"cool_data", 1, schema, 100, example_metadata
            )

def test_missing_repo():
    with temporary_directory() as tmpdir:
        config = {
            "user": "Dr Nose",
            "path": os.path.join(tmpdir, "repo"),
            "db_echo": False,
            "server": "localhost://",
        }
        assert_raises(NoRepository, Valet, config)

if __name__ == '__main__':
    test_valet_double_schema()