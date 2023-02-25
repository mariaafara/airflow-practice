import csv
from pathlib import Path


def load_csv(csv_path: Path):
    data = []
    with open(csv_path, "r", encoding="utf-8") as file:
        csv_reader = csv.reader(file, delimiter=",")
        next(csv_reader, None)  # skip the header
        for row in csv_reader:
            data.append({
                "text": row[0],
                "label": row[1]
            })
    return data


