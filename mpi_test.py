from mpi4py.MPI import COMM_WORLD
import numpy as np

from abbey import Valet

valet = Valet("./config.yml", COMM_WORLD)

schema = valet.get_schema("test_schema")
metadata = {"magic":14, "cake":45.6}
dataset = valet.create_dataset("my_parallel_dataset", 4, schema, 100, metadata)


if COMM_WORLD.Get_rank()==0:
    chunk = [
        np.arange(50), # coadd_object_id
        np.random.randn(50), #e1
        np.random.randn(50), #e2
    ]

    dataset.write(chunk, section_name="shape_cat", range=(0,50))
elif COMM_WORLD.Get_rank()==1:

    chunk = [
        np.arange(50), # coadd_object_id
        10*np.random.randn(50), #e1
        10*np.random.randn(50), #e2
    ]
    dataset.write(chunk, section_name="shape_cat", range=(50,100))
