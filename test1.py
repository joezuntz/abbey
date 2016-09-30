import abbey
import numpy as np


def create_test_schema(steward):
    cols = [
        ("shape_cat", "coadd_object_id", "int64"),
        ("shape_cat", "e1", "float64"),
        ("shape_cat", "e2", "float64"),
        ("sky_map", "pixel", "int64"),
        ("sky_map", "value", "int64"),
    ]

    required_metadata = [
        ("magic", "int64"),
        ("cake", "float64"),
    ]

    schema = abbey.Schema("test_schema", 1, cols, required_metadata)
    steward.register_schema(schema)
    return schema

def create_dataset(valet, schema):
    size = 100
    path = "./tmp.h5"
    metadata = {"magic": 15, "cake": 5.6}

    chunk = [
        np.arange(100), # coadd_object_id
        np.random.randn(100), #e1
        np.random.randn(100), #e2
    ]
    dataset = valet.create_dataset("temp", 5, schema, size, metadata)
    dataset.write(chunk, section_name="shape_cat")
    return dataset


valet = abbey.Valet("./config.yml")
# valet.print_dataset_list()
# create_test_schema(valet)
schema = valet.get_schema("test_schema")
# print schema
dataset = create_dataset(valet, schema)
dataset.resize((300,), "shape_cat")

# dataset = valet.open_dataset("not_real", schema, 1)
# dataset = valet.open_dataset("temp", schema)

# downton.steward.list_dataset_info()
# info = infos[0]
# dataset = downton.steward.open_dataset(info)
# print info
# print dataset
# print dataset.read(section_name="shape_cat")
# print info.creator
# schema = steward.get_schema('test_schema')
# create_dataset(schema)


# dataset = abbey.Dataset("./tmp.h5", schema, "r")
# data = dataset.read("shape_cat", range=(0,20))
# print data