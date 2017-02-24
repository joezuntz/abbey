from abbey import Dataset, Schema, Valet
import os
import numpy as np
from test_utils import temporary_directory, schema_ingredients, create_test_schema


def test_dataset_keys():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        assert dataset.has_column("e1", "shape_cat")
        dataset.close()


def test_dataset_reopen():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        dataset.close()

        dataset = Dataset(path, schema, "r")


def test_dataset_read_and_write():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        dataset.write([1+np.arange(100.), np.arange(100.)], columns=['e1','e2'], section_name='shape_cat')
        dataset.close()

        dataset = Dataset(path, schema, "rw")
        data = dataset.read(columns=['e1'], section_name='shape_cat')
        assert (data['e1']==1+np.arange(100.)).all()
        assert dataset.metadata['cake'] == 12.4
        data['e1'] *= -1
        dataset.write([data['e1']], columns=["e1"], section_name='shape_cat')
        #re-read immediately
        dataset.read(columns=['e1'], section_name='shape_cat')
        assert (data['e1']<0).all()
        dataset.close()
        #and re-read again later after closing to check persistence
        dataset = Dataset(path, schema, "r")
        data = dataset.read(columns=['e1'], section_name='shape_cat')
        assert (data['e1']<0).all()





def test_dataset_read_and_write_one_col():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        e1 = np.arange(100.)
        dataset.write(e1, columns='e1', section_name='shape_cat')
        dataset.close()
        dataset = Dataset(path, schema, "r")        
        x = dataset.read(columns=['e1'], section_name='shape_cat')
        assert (x['e1']==np.arange(100.)).all()
        assert dataset.metadata['cake'] == 12.4

def test_dataset_read_then_write():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        dataset.write([np.arange(100.), np.arange(100.)], columns=['e1','e2'], section_name='shape_cat')
        dataset.close()
        dataset = Dataset(path, schema, "r")        
        x = dataset.read(columns=['e1'], section_name='shape_cat')
        assert (x['e1']==np.arange(100.)).all()
        assert dataset.metadata['cake'] == 12.4



def test_meta():
    schema, metadata = create_test_schema()

    with temporary_directory() as tmpdir:
        path = os.path.join(tmpdir, "dataset.h5")
        dataset = Dataset(path, schema, "w", size=100, metadata=metadata)
        dataset.metadata['spoon'] = 'abc'
        dataset.metadata['fork'] = 99
        dataset.metadata['knife'] = 45.4
        dataset.close()

        dataset = Dataset(path, schema, "r")        
        assert dataset.metadata['spoon'] == 'abc'
        assert dataset.metadata['fork'] == 99
        assert dataset.metadata['knife'] == 45.4

