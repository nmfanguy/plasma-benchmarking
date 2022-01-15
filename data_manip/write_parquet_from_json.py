import pyarrow.parquet as pq
from pyarrow import json
import pyarrow as pa

table = json.read_json("in/huge_test_data.json")

pq.write_table(table, "in/huge_test_data.parquet")

