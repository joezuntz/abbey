Roadmap
=======

General Notes
-------------

* Data Management - just a table somewhere! On a github repo or something?
* Schemas 
    - approaches have often failed by being too general
    - what have others done? can we make this some subset?
* Data storage class
    - Use Pandas for this?  Can serialize etc.
* Additional validation beyond just data structure.  e.g. radians vs degrees!
    - Arbitrarty checks?


Tasks
-----

* Provenance creation and maintenance
* Documentations
* Schema documenting/describing themselves - "here is what I expect"
* (Additional) schema validation
* Github page with examples of current schema
* Choosing and implementing a strategy for syncing/globally tracking
* Moving datasets between machines
* Testing HDF5 parallel write!
* Representation-independence (pandas, recarrays, astropy tables)
* Backend choice/changes?


Hack Triage
------------

- Modify data type selection from string to type
- Write some schemas for WL Pipeline
    - Weights for n(z)
    - Shape catalogs

