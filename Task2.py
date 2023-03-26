from framework import map_reduce
from typing import List, Tuple, Callable, Dict

# Defines the data directories containing the CSV datasets
user_dir = "data/users"
click_dir = "data/clicks"

# Define the output directory where the output CSV file will be saved
output_dir = "data/filtered_clicks"

#Define the list of column titles. The first column in this case is also used as the key for mapping
user_output_columns = ["id", "country"]
output_columns = ["user_id", "count"]

#Define the map and reduce functions
def user_func(row: Dict,key) -> List[Tuple[str, int]]:
    if 'LT' in row:
        return [(row[key],'LT')]
    else:
        return []

def click_func(row: Dict,key) -> List[Tuple[str, int]]:
    return [(row[key], 1)]

def reduce_func(item: Tuple[str, int]) -> List[Tuple[str, int]]:
    user_id, count = item
    return [(user_id, count)]

# Call the map_reduce function
merge_info = map_reduce(user_dir, {user_func}, reduce_func, None, user_output_columns)
print(merge_info)
result = map_reduce(click_dir, {click_func}, reduce_func, output_dir, output_columns, merge_info)
print(result)