import numpy as np
import os
import pyarrow as pa
from pyarrow import csv
from pyarrow import json
import pyarrow.parquet as pq
import pyarrow.plasma as plasma

local = pa.fs.LocalFileSystem()

def random_obj_id() -> plasma.ObjectID:
    ''' Generate a new random plasma object ID using numpy. '''
    return plasma.ObjectID(np.random.bytes(20))

class Dataset:
    def __init__(self, filename: str, client: plasma.PlasmaClient):
        self.client = client

        self.is_huge = "huge_" in filename
        self.type = filename.split(".")[1]
        self.can_use_file = self.type == "csv" or self.type == "json"
        self.filename = filename
        self.out_filename = filename.replace(".", "_out.").replace("in", "out")

        self.file = None
        self.file_bytes = os.path.getsize(filename)

        self.stream = None 
        self.stream_bytes = self.file_bytes 

        self.table = None

        if self.type == "csv":
            self.table = csv.read_csv(filename)
        elif self.type == "json":
            self.table = json.read_json(filename)
        elif self.type == "parquet":
            self.table = pq.read_table(filename, memory_map=True)

        mock_sink = pa.MockOutputStream()

        with pa.RecordBatchStreamWriter(mock_sink, self.table.schema) as stream_writer:
                stream_writer.write_table(self.table)

        self.table_bytes = mock_sink.size()


    def roundtrip_file(self):
        if not self.can_use_file:
            return

        if self.file is None:
            self.file = open(self.filename, "rb")

        obj_id = random_obj_id()
        
        buf = memoryview(self.client.create(obj_id, self.file_bytes))
        
        for i, data in enumerate(self.file.read()):
            buf[i] = data
        
        self.client.seal(obj_id)

        with open(self.out_filename, "wb") as out_file:
            [buf] = self.client.get_buffers([obj_id])
            out_file.write(buf)

    
    def roundtrip_stream(self):
        if self.stream is None:
            self.stream = local.open_input_stream(self.filename)

        obj_id = random_obj_id()

        buf = memoryview(self.client.create(obj_id, self.stream_bytes))
        self.stream.readinto(buf)
        self.client.seal(obj_id)

        with local.open_output_stream(self.out_filename) as stream:
            [buf] = self.client.get_buffers([obj_id])
            stream.write(buf)

    
    def roundtrip_table(self):
        obj_id = random_obj_id()

        buf = self.client.create(obj_id, self.table_bytes)
        stream = pa.FixedSizeBufferWriter(buf)
        
        with pa.RecordBatchStreamWriter(stream, self.table.schema) as stream_writer:
            stream_writer.write_table(self.table)

        self.client.seal(obj_id)

        [data] = self.client.get_buffers([obj_id])
        buf = pa.BufferReader(data)
        reader = pa.RecordBatchStreamReader(buf)
        reader.read_all()
        

    def __del__(self):
        if self.file is not None:
            self.file.close()

        if self.stream is not None:
            self.stream.close()

    