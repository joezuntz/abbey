from abbey import Dataset, Schema, Valet
import tempfile
import contextlib
import os
import shutil
import numpy as np

@contextlib.contextmanager
def temporary_directory():
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir)


def schema_ingredients():
    columns = [
        ("shape_cat", "coadd_object_id", "int64"),
        ("shape_cat", "e1", "float64"),
        ("shape_cat", "e2", "float64"),
        ("sky_map", "pixel", "int64"),
        ("sky_map", "value", "int64"),
    ]

    required_metadata = [
        ("magic", "int64"),
        ("cake", "float64"),
        ("spoon", "str")
    ]
    example_metadata = {"magic":100, "cake":12.4, "spoon":"ladle"}
    return columns, required_metadata, example_metadata

def create_test_repo(tmpdir):
    config = {
        "user": "Dr Nose",
        "path": os.path.join(tmpdir, "repo"),
        "db_echo": False,
        "server": "localhost://",
    }
    Valet.create_repository(config)
    return Valet(config)


def create_test_schema():
    columns, required_metadata, example_metadata = schema_ingredients()
    schema = Schema("test_schema", 1, columns, required_metadata)
    return schema, example_metadata
