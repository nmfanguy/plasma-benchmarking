#! /usr/bin/python3

import numpy as np
import os
import pyarrow as pa
from pyarrow import csv
from pyarrow import json
import pyarrow.parquet as pq
import pyarrow.plasma as plasma
from rich import print
from rich.console import Console
from rich.progress import BarColumn, Progress
from rich.table import Column, Table
import sys
import timeit

local = pa.fs.LocalFileSystem()

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
    '''
    Roundtrip a native Python file object's bytes to the Plasma store.
    '''
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
    '''
    Roundtrip a PyArrow file stream's bytes to the Plasma store.
    '''
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


def roundtrip_mem_table(client: plasma.PlasmaClient, table: pa.Table):
    '''
    Roundtrip an in-memory PyArrow Table to the Plasma store.
    '''
    obj_id = random_obj_id()

    mock_sink = pa.MockOutputStream()

    with pa.RecordBatchStreamWriter(mock_sink, table.schema) as stream_writer:
            stream_writer.write_table(table)

    data_size = mock_sink.size()

    # write the parquet in-memory table
    buf = client.create(obj_id, data_size)
    stream = pa.FixedSizeBufferWriter(buf)
    
    with pa.RecordBatchStreamWriter(stream, table.schema) as stream_writer:
            stream_writer.write_table(table)

    client.seal(obj_id)

    # read the parquet table from Plasma & create in-memory table
    [data] = client.get_buffers([obj_id])
    buf = pa.BufferReader(data)

    reader = pa.RecordBatchStreamReader(buf)

    reader.read_all()


def time_and_output(out: Table, name: str, num: int, func):
    '''
    Time the specified function and output the results to the given Table. `name` 
    and `num` specify what to put in the output table columns.
    '''
    exec_time = timeit.timeit(func, number=num)

    avg_time = exec_time / num

    out.add_row(name, str(num), str(exec_time), str(avg_time))


def add_header_row(out: Table, title: str):
    out.add_row("[cyan]>>> {0} <<<[/]".format(title), "", "", "")


def print_help():
    '''
    Print help information about this program.
    '''
    print()
    print("[green]USAGE[/]: plasma-benchmarking.py [red][OPTIONS][/]")
    print("Where [red][OPTIONS][/] := ")
    print("\t[cyan]-h[/] | [cyan]--help[/] - display this help, then exit")
    print("\t[cyan]-o[/] | [cyan]--omit-huge[/] - omit sending \"huge\" data files; useful for reducing execution time")


def main():
    check_huge_files = True

    # parse command-line args
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print_help()
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] in ["-o", "--omit-huge"]:
        check_huge_files = False
 
    print("*** [green u]Starting the Plasma Benchmark[/] ***")  
  
    # connect to the running plasma server
    start = timeit.default_timer()
    client = plasma.connect("/tmp/plasma")
    end = timeit.default_timer()  

    console = Console()
    out = Table(Column("Operation", justify="center"), 
        Column("Reps", justify="center"), 
        "Total Time (s)", 
        "Average Time (s)", 
        show_header=True, 
        header_style="cyan")
    
    with Progress("{task.description}", BarColumn(), "{task.percentage:>3.0f}%") as progress:
        benchmark = progress.add_task("Working...", total=18 if check_huge_files else 16)
    
        out.add_row("create client", str(1), str(end - start), str(end - start))
        progress.update(benchmark, advance=1)
        time_and_output(out, "connection check", 1000, lambda: check_connection(client))
        progress.update(benchmark, advance=1)

        add_header_row(out, "FILES")
        time_and_output(out, "csv", 1000, lambda: roundtrip_file(client, "in/test_data.csv"))
        progress.update(benchmark, advance=1)
        time_and_output(out, "json", 1000, lambda: roundtrip_file(client, "in/test_data.json"))
        progress.update(benchmark, advance=1)
     
        add_header_row(out, "HUGE FILES")
        if check_huge_files:
            time_and_output(out, "csv", 100, lambda: roundtrip_file(client, "in/huge_test_data.csv"))
            progress.update(benchmark, advance=1)
            time_and_output(out, "json", 100, lambda: roundtrip_file(client, "in/huge_test_data.json"))
            progress.update(benchmark, advance=1)
        else:
            out.add_row("[red]omitted[/]", "", "", "")    
 
        add_header_row(out, "STREAMS")
        time_and_output(out, "csv", 1000, lambda: roundtrip_file_stream(client, "in/test_data.csv"))
        progress.update(benchmark, advance=1)
        time_and_output(out, "json", 1000, lambda: roundtrip_file_stream(client, "in/test_data.json"))
        progress.update(benchmark, advance=1)
        time_and_output(out, "parquet", 1000, lambda: roundtrip_file_stream(client, "in/test_data.parquet"))
        progress.update(benchmark, advance=1)
       
        add_header_row(out, "HUGE STREAMS")
        time_and_output(out, "csv", 100, lambda: roundtrip_file_stream(client, "in/huge_test_data.csv"))
        progress.update(benchmark, advance=1)
        time_and_output(out, "json", 100, lambda: roundtrip_file_stream(client, "in/huge_test_data.json"))
        progress.update(benchmark, advance=1)
        time_and_output(out, "parquet", 100, lambda: roundtrip_file_stream(client, "in/huge_test_data.parquet"))
        progress.update(benchmark, advance=1)

        # this is not included in the computation time -- read in all the in-mem tables
        regular_csv_table = csv.read_csv("in/test_data.csv")       
        huge_csv_table = csv.read_csv("in/huge_test_data.csv")
        regular_pq_table = pq.read_table("in/test_data.parquet", memory_map=True)
        huge_pq_table = pq.read_table("in/huge_test_data.parquet", memory_map=True)
        regular_json_table = json.read_json("in/test_data.json")
        huge_json_table = json.read_json("in/huge_test_data.json")

        add_header_row(out, "IN-MEMORY OBJS")
        time_and_output(out, "csv", 1000, lambda: roundtrip_mem_table(client, regular_csv_table))
        progress.update(benchmark, advance=1)
        time_and_output(out, "json", 1000, lambda: roundtrip_mem_table(client, regular_json_table))
        progress.update(benchmark, advance=1)
        time_and_output(out, "parquet", 1000, lambda: roundtrip_mem_table(client, regular_pq_table))
        progress.update(benchmark, advance=1)

        add_header_row(out, "HUGE IN-MEMORY OBJS")
        time_and_output(out, "csv", 1000, lambda: roundtrip_mem_table(client, huge_csv_table))
        progress.update(benchmark, advance=1)
        time_and_output(out, "json", 1000, lambda: roundtrip_mem_table(client, huge_json_table))
        progress.update(benchmark, advance=1)   
        time_and_output(out, "parquet", 1000, lambda: roundtrip_mem_table(client, huge_pq_table))
        progress.update(benchmark, advance=1)
 
    console.print(out)

    print("*** [green u]Ending the Plasma Benchmark[/] ***")

    # finally, disconnect the client
    client.disconnect()


if __name__ == "__main__":
    main()
