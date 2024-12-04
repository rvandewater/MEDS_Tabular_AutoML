import os
import numpy as np
import csv

def get_npz_array_lengths(directory):
    array_lengths = {}

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.npz'):
                file_path = os.path.join(root, file)
                if os.path.getsize(file_path) > 1 * 1024 * 1024:  # Skip files larger than 1MB
                    continue
                npz_file = np.load(file_path)
                array_name = npz_file.files[0]
                array_lengths[f"{file_path}:{array_name}"] = npz_file[array_name].size
    return array_lengths

directory = "/dhc/home/robin.vandewater/datasets/AUMCdb_1.0.2_MEDS_TAB/TAB"
lengths = get_npz_array_lengths(directory)

output_csv = "/dhc/home/robin.vandewater/datasets/array_lengths.csv"
with open(output_csv, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["File: Array", "Length"])
    for key, value in lengths.items():
        writer.writerow([key, value])

print(f"Array lengths have been saved to {output_csv}")