# MapReduce Framework using Higher Order Functions
import os
import csv
from typing import List, Tuple, Callable, Dict

def mapper(file_path: str, map_func: Callable, column_name: str) -> List:
    #Applies the map_func to the column of each row
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        column_names = next(reader)
        column_index = column_names.index(column_name)
        for row in reader:
            data.extend(map_func(row[column_index]))
    return data


def reducer(data: List, reduce_func: Callable) -> Dict:
    #Applies the reduce_func to every element in the data list
    result = {}
    for item in data:
        result = reduce_func(result, item)
    return result

def map_reduce(data_dir: str, map_func: Callable, reduce_func: Callable, output_dir: str, output_columns: List[str]) -> Dict:
    #Applies the map_func to every row in every CSV file in the data_dir directory, then applies the reduce_func to the result and outputs it to a CSV file
    column_name=output_columns[0]
    mapped_data = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(data_dir, filename)
            mapped_data.extend(mapper(file_path, map_func,column_name))
    reduced_data = reducer(mapped_data, reduce_func)
    # Write the reduced data to a CSV file
    output_file = output_dir+".csv"
    with open(output_file, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_columns)
        writer.writeheader()
        for key, value in reduced_data.items():
            writer.writerow({output_columns[0]: key, output_columns[1]: value})
    return reduced_data