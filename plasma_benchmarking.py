#! /usr/bin/python3

from dataset import Dataset
import os
import pyarrow.plasma as plasma
from rich import print
from rich.console import Console
from rich.table import Table
import sys
import timeit

def print_help():
    '''
    Print help information about this program.
    '''
    print()
    print("[green]USAGE[/]: plasma-benchmarking.py [red][OPTIONS][/]")
    print("Where [red][OPTIONS][/] := ")
    print("\t[cyan]-h[/] | [cyan]--help[/] - display this help, then exit")
    print("\t[cyan]-table[/] | [cyan]--omit-huge[/] - omit sending \"huge\" data files; useful for reducing execution time")


def main():
    check_huge_files = True
    REPS = 1000
    WARMUPS = 100

    # parse command-line args
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        print_help()
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] in ["-o", "--omit-huge"]:
        check_huge_files = False
 
    print("\n*** [cyan]Starting the Plasma Benchmark[/] ***")  
  
    # set up output tables
    console = Console()
    files = Table("File", "Type", "Bytes/Transfer", "Reps", "Total Time (s)", "Time/Byte (s)")
    streams = Table("File", "Type", "Bytes/Transfer", "Reps", "Total Time (s)", "Time/Byte (s)")
    tables = Table("File", "Type", "Bytes/Transfer", "Reps", "Total Time (s)", "Time/Byte (s)")

    # connect to the running plasma server
    client = plasma.connect("/tmp/plasma")

    # get all the datasets in the "in" folder
    in_files = os.listdir("in")
    datasets = []

    # and create a Dataset object for each file
    for file in in_files:
        file = "in/" + file
        datasets.append(Dataset(file, client))

    # process each dataset
    runs = 0
    for d in datasets:
        if not check_huge_files and d.is_huge:
            continue

        print("{0}...".format(d.filename), end="")
        sys.stdout.flush()

        n = REPS - WARMUPS

        # parquet files can't use the Python file API, so skip this check for those
        if d.can_use_file:
            warmup = timeit.timeit(d.roundtrip_file, number=100)
            t = timeit.timeit(d.roundtrip_file, number=n)
            time_per = t / (d.file_bytes * n)
            files.add_row(d.filename, d.type.upper(), str(d.file_bytes), str(n), str(t), str(time_per))

        warmup = timeit.timeit(d.roundtrip_stream, number=100)
        t = timeit.timeit(d.roundtrip_stream, number=n)
        time_per = t / (d.file_bytes * n)
        streams.add_row(d.filename, d.type.upper(), str(d.stream_bytes), str(n), str(t), str(time_per))

        warmup = timeit.timeit(d.roundtrip_table, number=100)
        t = timeit.timeit(d.roundtrip_table, number=n)
        time_per = t / (d.file_bytes * n)
        tables.add_row(d.filename, d.type.upper(), str(d.table_bytes), str(n), str(t), str(time_per))

        print(" [green]done[/]!")

    # finally, print all the output
    print()
    print("[cyan]FILES[/]")
    console.print(files)
    
    print()
    print("[cyan]STREAMS[/]")
    console.print(streams)
    
    print()
    print("[cyan]IN-MEM TABLES[/]")
    console.print(tables)

    client.disconnect()
    print("*** [cyan]Ending the Plasma Benchmark[/] ***")


if __name__ == "__main__":
    main()
