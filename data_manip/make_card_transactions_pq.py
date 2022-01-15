from pyarrow import csv
import pyarrow.parquet as pq

csv_table = csv.read_csv("../in/huge_card_transactions.csv")
pq.write_table(csv_table, "../in/huge_card_transactions.parquet")