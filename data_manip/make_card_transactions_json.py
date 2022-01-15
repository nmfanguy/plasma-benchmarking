from pyarrow import csv

csv_table = csv.read_csv("../in/card_transactions.csv")
csv_pandas = csv_table.to_pandas()
csv_pandas.to_json("../in/card_transactions.json", orient="records", lines=True)
