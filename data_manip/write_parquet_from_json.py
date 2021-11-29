import pyarrow.parquet as pq
from pyarrow import json
import pyarrow as pa

table = json.read_json("in/huge_test_data.json")

pandas_table = table.to_pandas()

pa_table = pa.Table.from_pandas(pandas_table)

print(pa_table)

pq.write_table(pa_table, "in/huge_test_data.parquet")

