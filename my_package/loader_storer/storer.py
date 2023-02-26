import csv
from pathlib import Path


def store_csv(path: Path, data):
    # Write list of dictionaries to CSV file
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        for d in data:
            writer.writerow(d)
