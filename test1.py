import abbey
import numpy as np
import argparse
import sys

def create_test_schema(valet):
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

    schema = valet.create_schema("test_schema", 1, cols, required_metadata)
    return valet.get_schema("test_schema", 1)



parser = argparse.ArgumentParser(description="Test Abbey")
parser.add_argument("--create-repo",  action='store_true', help="Create the repository")
parser.add_argument("--create-dataset", help="Create a dataset with the given name")
parser.add_argument("--read",    help="Read the named dataset")
parser.add_argument("--missing", action='store_true', help="Make an error by asking for a missing dataset")
parser.add_argument("--version", type=int, default=1,  help="Version of the data set to create/read")
parser.add_argument("--list",    action='store_true', help="List data sets")
parser.add_argument("--mpi",    action='store_true', help="Run under MPI")
args = parser.parse_args()

config = "./config.yml"

if args.mpi:
    import mpi4py.MPI
    comm = mpi4py.MPI.COMM_WORLD
    mpi_rank = comm.Get_rank()
    mpi_size = comm.Get_size()
else:
    comm = None


def example_create_repo():
    abbey.Valet.create_repository(config)
    valet = abbey.Valet(config)
    create_test_schema(valet)


def example_get_valet():
    valet = abbey.Valet(config, comm)
    schema = valet.get_schema("test_schema")
    return valet,schema

def example_create_dataset():
    size = 100
    metadata = {"magic": 15, "cake": 5.6}

    chunk = [
        np.arange(100), # coadd_object_id
        np.random.randn(100), #e1
        np.random.randn(100), #e2
    ]
    dataset = valet.create_dataset(args.create_dataset, args.version, schema, size, metadata)
    dataset.write(chunk, section_name="shape_cat")

def example_read_dataset():
    dataset = valet.open_dataset(args.read, schema, args.version)
    start = mpi_rank*10
    end = start + 10
    print dataset.read("shape_cat", range=(start,end))

def example_missing():
    dataset = valet.open_dataset("not_real", schema, args.version)

def example_list():
    valet.print_all()


if args.create_repo:
    example_create_repo()
    sys.exit(0)
else:
    #The other examples all need a created valet
    valet, schema = example_get_valet()

if args.create_dataset:
    example_create_dataset()
elif args.read:
    example_read_dataset()
elif args.list:
    example_list()
elif args.missing:
    example_missing()
