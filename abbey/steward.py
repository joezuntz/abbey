from .schema import Schema
from .dataset import Dataset
import os
import collections
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm 

Base = sqlalchemy.ext.declarative.declarative_base()


class ListEntry(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.types.Text
    def process_bind_param(self, value, dialect):
        return repr(value)
    def process_result_value(self, value, dialect):
        return eval(value)

class DatasetEntry(Base):
    __tablename__ = "datasets"
    name = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    version = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    schema_name = sqlalchemy.Column(sqlalchemy.String)
    schema_version = sqlalchemy.Column(sqlalchemy.Integer)
    creator = sqlalchemy.Column(sqlalchemy.String)
    path = sqlalchemy.Column(sqlalchemy.String)

    def __repr__(self):
        return '<Dataset Info "{}" v{}>'.format(self.name,self.version)



class SchemaEntry(Base):
    __tablename__ = "schemas"
    name = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    version = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    columns = sqlalchemy.Column(ListEntry)
    required_metadata = sqlalchemy.Column(ListEntry)

    def __repr__(self):
        return '<Schema Info "{}" v{}>'.format(self.name,self.version)

    def to_schema(self):
        return Schema(self.name, self.version, self.columns, self.required_metadata)

    @classmethod
    def from_schema(cls, schema):
        #some paranoid checking
        assert isinstance(schema.columns, list)
        for col in schema.columns:
            assert isinstance(col, tuple)
            assert len(col)==3
            assert isinstance(col[0], str)
            assert isinstance(col[1], str)
            assert isinstance(col[2], str)
        assert isinstance(schema.columns, list)

        for col in schema.required_metadata:
            assert isinstance(col, tuple)
            assert len(col)==2
            assert isinstance(col[0], str)
            assert isinstance(col[1], str)

        return cls(name=schema.name, version=schema.version, columns=schema.columns, required_metadata=schema.required_metadata)



class Steward(object):
    """
    A register of data sets and schema. Synchronized with disk.
    Should be version controllable in some way.

    """
    def __init__(self, config, mpi_comm=None):
        self.user = config.user
        self.path = config.path
        if not os.path.exists(self.db_path):
            raise ValueError("Repository not found at {} (missing repo.db)".format(self.path))
        if not os.path.exists(self.data_path):
            raise ValueError("Repository not found at {} (missing data dir)".format(self.path))
        engine_path = 'sqlite:///' + self.db_path
        self.engine = sqlalchemy.create_engine(engine_path, echo=config.db_echo)
        Base.metadata.create_all(self.engine)
        Session = sqlalchemy.orm.sessionmaker()
        Session.configure(bind=self.engine)
        self.db = Session()
        self.mpi_comm = mpi_comm
        if self.mpi_comm is None:
            self.mpi_rank = 0
            self.mpi_size = 1
        else:
            self.mpi_rank = mpi_comm.Get_rank()
            self.mpi_size = mpi_comm.Get_size()

    @property
    def is_master(self):
        return self.mpi_rank==0

    def path_for_dataset(self, name, version):
        return os.path.join(self.data_path, "{}_{}.h5".format(name, version))

    @property
    def data_path(self):
        return os.path.join(self.path, "data")

    @property
    def db_path(self):
        return os.path.join(self.path, "repo.db")

    def register_schema(self, schema):
        if not self.is_master:
            return
        entry = SchemaEntry.from_schema(schema)
        self.db.add(entry)
        self.db.commit()

    def get_schema(self, name, version=None):
        if version is None:
            entry = self.db.query(SchemaEntry).filter_by(name=name).order_by(
                sqlalchemy.desc(SchemaEntry.version)).first()
        else:
            entry = self.db.query(SchemaEntry).filter_by(name=name, version=version).first()
        schema = entry.to_schema()
        return schema

    def create_schema(self, name, version, columns, required_metadata):
        if not self.is_master:
            return
        entry = SchemaEntry(name = name,
            version = version,
            columns = columns,
            required_metadata = required_metadata
            )
        self.db.add(entry)
        self.db.commit()


    def create_dataset(self, name, version, schema, size, metadata):
        creator = self.user
        path = self.path_for_dataset(name, version)
        dataset = Dataset(path, schema, "w", size, metadata, comm=self.mpi_comm)
        #Only register the dataset after creating it in case something goes wrong.
        if self.is_master:
            entry = DatasetEntry(name = name,
                version = version,
                schema_name = schema.name,
                schema_version = schema.version,
                creator = creator,
                path = path
            )
            self.db.add(entry)
            self.db.commit()
        return dataset

    def delete_schema(self, name, version):
        entry = self.db.query(SchemaEntry).filter_by(name=name, version=version).first()
        if entry is None:
            raise ValueError("Schema you want to delete ({} v{}) not found.  Maybe that is good.".format(name,version))
        self.db.delete(entry)
        self.db.commit()

    def delete_dataset(self, name, version):
        path = self.path_for_dataset(name, version)
        if os.path.exists(path):
            os.remove(path)
        entry = self.db.query(DatasetEntry).filter_by(name=name, version=version).first()
        if entry is None:
            raise ValueError("Dataset you want to delete ({} v{}) not found.  Maybe that is good.".format(name,version))
        self.db.delete(entry)
        self.db.commit()



    def open_dataset(self, info, schema=None):
        schema2 = self.get_schema(info.schema_name, info.schema_version)
        if schema is not None:
            if schema!=schema2:
                print schema==schema2
                raise ValueError("Schema {} of dataset {} does not match requested schema {}".format(
                    schema2, info, schema))

        path = self.path_for_dataset(info.name, info.version)
        dataset = Dataset(path, schema, "r")
        return dataset


    def get_dataset_info(self, name, version=None):
        if version is None:
            entry = self.db.query(DatasetEntry).filter_by(name=name).order_by(
                sqlalchemy.desc(DatasetEntry.version)).first()
        else:
            entry = self.db.query(DatasetEntry).filter_by(name=name, version=version).first()
        return entry

    def list_dataset_info(self, name=None, version=None, schema=None, schema_name=None, schema_version=None,creator=None):
        if schema is not None and (schema_name is not None or schema_version is not None):
            raise ValueError("You can search for either a specific schema or schema_name/schema_version but not both (the former implies both the latter)")
        entries = self.db.query(DatasetEntry)
        if name is not None:
            entries = entries.filter_by(name=name)
        if version is not None:
            entries = entries.filter_by(version=version)
        if schema_name is not None:
            entries = entries.filter_by(schema_name=schema_name)
        if schema_version is not None:
            entries = entries.filter_by(schema_version=schema_version)
        if creator is not None:
            entries = entries.filter_by(creator=creator)
        if schema is not None:
            entries = entries.filter_by(schema_name=schema.name, schema_version=schema.version)
        return list(entries)

    def list_schema_info(self, name=None, version=None):
        entries = self.db.query(SchemaEntry)
        if name is not None:
            entries = entries.filter_by(name=name)
        if version is not None:
            entries = entries.filter_by(version=version)
        return list(entries)




    def ingest_dataset(self, name, version, schema, path, creator):
        """Copy a dataset into the repository"""
        pass



