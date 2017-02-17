Abbey
-----


Abbey is a prototype for a python I/O and (light) data management library for
table-like data in cosmology pipelines.

It is not yet ready for any real use.

Goals
-----

- Mediate access to table-like data sets
- Manage storing, versioning, and listing them
- Validate them against schema
- Coordinate them between machines
- Work under MPI or other parallel access


API: Valet & Dataset
--------------------

The only two classes that user code should need to interact with are 
abbey.Valet and abbey.Dataset.

The Valet is in charge of loading and saving Datasets in the correct place and
synchronizing them with remotes. The Dataset gives direct and efficient access
to data in files.

Valet
-----

The valet provides methods for opening, creating, listing, and deleting 
Datasets, and syncing them with a remote system.

Valet's implementation calls out to the other objects (Steward, Dataset, 
Footman).

Dataset
------

The Dataset object provides read and write access to data files. The format of
the data files is currently HDF5, but we will look into this.

It provides read and write access to chunks of the data, column and row-wise.


Steward
-----

abbey.Steward is responsible for maintaining the database of data sets and schemas, and their paths on disc.  
All database access goes through the Steward to make it easier to modify cleanly.

The current implementation uses sqlalchemy and a sqlite database.


