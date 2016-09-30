import glob
import argparse
import fitsio
import abbey
#

parser = argparse.ArgumentParser(description="Convert a large group of catalog FITS files into a single big HDF5 file.")
parser.add_argument("data_repo_config", help="Configuration file for data repository")
parser.add_argument("name", help="Name of the new dataset")
parser.add_argument("version", type=int, help="Version number of the new dataset")
parser.add_argument("directory", help="The path where the input fits files can be found")
parser.add_argument("--mpi", action="store_true", help="Run under MPI")

if args.mpi:
    from mpi4py.MPI import COMM_WORLD
    comm = COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
else:
    comm = None
    rank = 0
    size = 1 

#Collect all files to run
files = glob.glob(args.directory+"/*.fits")
files.sort()

#count the total size of all the files.
file_sizes = []
total = 0
if rank==0:
    file_sizes = [fitsio.FITS(filename)[1].get("NAXIS2") for filename in files]
if comm:
    comm.bcast(file_sizes)
total = sum(file_sizes)

#create our new data set
valet = abbey.Valet(args.data_repo_config, mpi_comm=comm)
schema = valet.get_schema("shape_catalog")
metadata = {}
dataset = valet.create_dataset(args.name, args.version, schema, total, metadata)

#create the extra columns we want in addition to the standard ones in the schema
row = fitsio.FITS(files[0])[1].read_rows([0])
col_names = r.dtype.names
dtypes = [r[name].dtype.str for name in col_names]
dataset.add_columns(col_names, dtypes)
keys = dataset.keys()

#Loop through adding the data to the file.
index = 0
for i, (filename, file_rows) in enumerate(zip(files, file_sizes)):
    #only do the files assigned to this process.
    if i%size!=rank:
        index += file_rows
        continue
    #Load this FITS file and paste in the data
    data = fitsio.FITS(filename)[1].read()
    #reorder to match the order in the output file
    data = [data[d] for d in keys]
    #write out the data
    dataset.write(data, range=(index, index+len(data[0])))
    index += file_rows
