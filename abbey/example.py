
def user_code_example():
    """
    This is the only class that most user applications have to interact with.
    It knows about the Valet.
    It can ask the Valet for an existing data set.
    It can read a chunk of an existing data set.
    It can be told about a new data sets
    """
    import abbey

    #Reads the configuration file in that directory
    downton = abbey.Abbey("/path/to/data/repository", mpi=False)
    valet = downton.get_valet()

    # get the latest version of the schema
    schema = valet.get_schema("shape_catalog")

    # just use a very simple name.
    # abbey does the rest - latest version, etc.
    name = "im3shape-r"
    version = 4
    shape_cat = valet.find(name, schema, version)
    # shape_cat is an object wrapping a reference to an HDF5 table somewhere.
    print shape_cat.version, shape_cat.human_readable_name, shape_cat.metadata

    for chunk in shape_cat.read_chunks():
        run_your_analysis(chunk)
    
    # read in the whole catalog, all columns and rows
    shape_cat.read()

    #read in a chunk of rows, all columns
    shape_cat.read_chunk()

    # read in one column, all rows
    cid = shape_cat.read("coadd_object_id")

    # read in chunk of rows, two columns
    e1, e2 = shape_cat.read_chunk(["e1", "e2"])

    # Do schemas have a hierarchy?
    # Or are they just unique things?
    # Schemas have some required metadata

    # Creating a new data set
    schema = downton.schemas.pz_catalog
    metadata = schema.metadata()

    # Fill in required metadata
    # this adds external links to the shape catalog columns
    # in the new catalog.
    metadata['parent'] = shape_cat
    metadata['code_name'] = 'BPZ'
    metadata['number_templates'] = 17

    # And some optional stuff too, designed for human reading later
    metadata['settings'] = '...'

    # Create a new data set
    pz_cat = valet.create_dataset(name, schema, metadata, size=size) #size is optional
    # valet discreetly checks that the required metadata is present.
    # or maybe that should happen at the end? If you are looping through.

    print pz_cat.id 
    print pz_cat.version #automatic based on previously known local versions.
    print pz_cat.date

    # This must work transparently in parallel!
    for i in xrange(n):
        pz_cat.write_chunk(chunk, start, end)  #start and end are required if in parallel

    pz_cat.close() # ?
    pz_cat.finalize()  # mark this as complete - no new data to be added.


    # can a dataset be opened again once finalized?
    # yes for appending, no modifications though??
    # should this create a new dataset?

