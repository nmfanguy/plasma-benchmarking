#! /usr/bin/python3

import csv
import numpy as np
import os
from pyarrow import fs
import pyarrow.plasma as plasma
import sys
import timeit

local = fs.LocalFileSystem()

def random_obj_id() -> plasma.ObjectID:
    '''
    Generate a new random plasma object ID using numpy. 
    '''
    return plasma.ObjectID(np.random.bytes(20))


def check_connection(client: plasma.PlasmaClient) -> bool:
    '''
    Check the client's connection by putting & getting a string,
    making sure the gotten string matches the put one.    
    '''
    id = client.put("testing")
    obj = client.get(id)

    return obj == "testing" 


def roundtrip_file(client: plasma.PlasmaClient, filename: str):
    obj_id = random_obj_id()
    # replace end of filename w/ _out & change directory from in to out
    out_filename = filename.replace(".", "_out.").replace("in", "out")
    
    # write the file to plasma
    with open(filename, "rb") as in_file:
        file_bytes = in_file.read()
        buf = memoryview(client.create(obj_id, len(file_bytes)))
        
        for i, data in enumerate(file_bytes):
            buf[i] = data

        client.seal(obj_id)

    # read the file back from plasma
    with open(out_filename, "wb") as out_file: 
        [buf] = client.get_buffers([obj_id])
        out_file.write(buf)    


def roundtrip_file_stream(client: plasma.PlasmaClient, filename: str):
    global local

    obj_id = random_obj_id()
    # replace end of filename w/ _out & change directory from in to out
    out_filename = filename.replace(".", "_out.").replace("in", "out")

    # write the file
    with local.open_input_stream(filename) as stream:
        buf = memoryview(client.create(obj_id, os.path.getsize(filename)))
      
        stream.readinto(buf)
  
        client.seal(obj_id)       

    # read the file
    with local.open_output_stream(out_filename) as stream:
        [buf] = client.get_buffers([obj_id])
    
        stream.write(buf)


def main():
    # connect to the running plasma server
    start = timeit.default_timer()
    client = plasma.connect("/tmp/plasma")
    end = timeit.default_timer()  

    # perform all the timeit operations
    connection_check_time = timeit.timeit(lambda: check_connection(client), number=10)

    csv_file_time  = timeit.timeit(lambda: roundtrip_file(client, "in/test_data.csv"),  number=10)
    json_file_time = timeit.timeit(lambda: roundtrip_file(client, "in/test_data.json"), number=10)
  
    csv_stream_time     = timeit.timeit(lambda: roundtrip_file_stream(client, "in/test_data.csv"),     number=10)
    json_stream_time    = timeit.timeit(lambda: roundtrip_file_stream(client, "in/test_data.json"),    number=10)
    parquet_stream_time = timeit.timeit(lambda: roundtrip_file_stream(client, "in/test_data.parquet"), number=10)

    huge_json_file_time = timeit.timeit(lambda: roundtrip_file(client, "in/huge_test_data.json"), number=1)
    huge_csv_file_time  = timeit.timeit(lambda: roundtrip_file(client, "in/huge_test_data.csv"),  number=1)
   
    huge_csv_stream_time     = timeit.timeit(lambda: roundtrip_file_stream(client, "in/huge_test_data.csv"),     number=1)
    huge_json_stream_time    = timeit.timeit(lambda: roundtrip_file_stream(client, "in/huge_test_data.json"),    number=1)
    huge_parquet_stream_time = timeit.timeit(lambda: roundtrip_file_stream(client, "in/huge_test_data.parquet"), number=1)
 
    # print out the results 
    print()
    print("Operation                 Total Time Taken (s)")
    print("----------                --------------------")
    print("create client             {0}".format(end - start))
    print("connection check  (x10)   {0}".format(connection_check_time))
    print("\n>>> files <<<")
    print("roundtrip csv     (x10)   {0}".format(csv_file_time))
    print("roundtrip json    (x10)   {0}".format(json_file_time))
    print("\n>>> streams <<<")
    print("roundtrip csv     (x10)   {0}".format(csv_stream_time))
    print("roundtrip json    (x10)   {0}".format(json_stream_time))
    print("roundtrip parquet (x10)   {0}".format(parquet_stream_time))
    print("\n>>> huge files <<<")
    print("roundtrip json (x1)  {0}".format(huge_json_file_time))
    print("roundtrip csv  (x1)  {0}".format(huge_csv_file_time))
    print("\n>>> huge streams <<<")
    print("roundtrip csv     (x1)   {0}".format(huge_csv_stream_time))
    print("roundtrip json    (x1)   {0}".format(huge_json_stream_time))
    print("roundtrip parquet (x1)   {0}".format(huge_parquet_stream_time))


    # messing around w/ parquet
    import pyarrow.parquet as pq
    import pyarrow as pa

    # write the parquet in-memory table
    obj_id = random_obj_id()
    pq_table = pq.read_table("in/test_data.parquet", memory_map=True)

    mock_sink = pa.MockOutputStream()

    with pa.RecordBatchStreamWriter(mock_sink, pq_table.schema) as stream_writer:
            stream_writer.write_table(pq_table)

    data_size = mock_sink.size()

    buf = client.create(obj_id, data_size)
    stream = pa.FixedSizeBufferWriter(buf)
    
    with pa.RecordBatchStreamWriter(stream, pq_table.schema) as stream_writer:
            stream_writer.write_table(pq_table)

    client.seal(obj_id)

    # read the parquet table from Plasma & create in-memory table
    [data] = client.get_buffers([obj_id])
    buf = pa.BufferReader(data)

    reader = pa.RecordBatchStreamReader(buf)

    pq_table = reader.read_all()

    print()
    print("Objects in the Store At End")
    print("----------------------------")
    print(len(client.list()))

    # finally, disconnect the client
    client.disconnect()


if __name__ == "__main__":
    main()
