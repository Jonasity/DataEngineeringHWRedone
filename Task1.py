from framework import map_reduce
from typing import List, Tuple, Callable, Dict

# Define the data directory containing the CSV files
data_dir = "data/clicks"

# Define the output directory where the output CSV file will be saved
output_dir = "data/total_clicks"

#Define the list of column titles for the output CSV file
output_columns = ["date", "count"]

#Define the map and reduce functions
def map_func(row: Dict) -> List[Tuple[str, int]]:
    #Maps the specified column in each row to a list of (value, 1) tuples
    return [(row, 1)]

def reduce_func(result: Dict, item: Tuple[str, int]) -> Dict:
    #Reduces a list of (date, count) tuples into a dictionary with the counts
    date, count = item
    if date in result:
        result[date] += count
    else:
        result[date] = count
    return result

# Call the map_reduce function
result = map_reduce(data_dir, map_func, reduce_func, output_dir, output_columns)

print(result)