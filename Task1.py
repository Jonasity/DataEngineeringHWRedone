from framework import map_reduce
from typing import List, Tuple, Callable, Dict

# Define the data directory containing the CSV files
data_dir = "data/clicks"

# Define the output directory where the output CSV file will be saved
output_dir = "data/total_clicks"

#Define the list of column titles for the output CSV file (First column functions as key for mapper)
output_columns = ["date", "count"]

#Define the map and reduce functions
def map_func(row: Dict,key) -> List[Tuple[str, int]]:
    return [(row[key], 1)]

def reduce_func(item: Tuple[str, int]) -> List[Tuple[str, int]]:
    date, count = item
    return [(date, count)]

result = map_reduce(data_dir, {map_func}, reduce_func, output_dir, output_columns)
print(result)