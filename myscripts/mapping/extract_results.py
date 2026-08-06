import json
import re
import sys
import argparse
import csv

def parse_query_exec_info(data: dict) -> tuple:
    """
    Parse a query_exec_info JSON dict and return:
    (query_id, dam_size, table_size, total_allocated_bytes_delta_pool2,
     total_allocated_bytes_delta_pool3, duration)
    """
    label = data["label"]
    m = re.fullmatch(r"d(\d+)_tf(\d+)_q(\d+)", label)
    if not m:
        raise ValueError(f"Unexpected label format: {label!r}")
    dam_size   = int(m.group(1))
    table_size = int(m.group(2))
    query_id   = int(m.group(3))

    duration = data["duration"]

    delta_pool2 = 0
    delta_pool3 = 0
    for entry in data.get("memory_deltas", []):
        if entry["resource_id"] == 2:
            delta_pool2 = entry["total_allocated_bytes_delta"]
        elif entry["resource_id"] == 3:
            delta_pool3 = entry["total_allocated_bytes_delta"]

    return (query_id, dam_size, table_size, delta_pool2, delta_pool3, duration)


def load_and_parse(path: str) -> tuple:
    with open(path) as f:
        return parse_query_exec_info(json.load(f))
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Extract query execution info from JSON file")
    parser.add_argument("files", nargs="+", help="JSON files to process")
    parser.add_argument("--output", "-o", default="results.csv", help="Output CSV file (default: results.csv)")
    
    args = parser.parse_args()
    
    results = []
    for file_path in args.files:
        try:
            result = load_and_parse(file_path)
            results.append(result)
        except Exception as e:
            print(f"Error processing {file_path}: {e}", file=sys.stderr)

    # Write results to CSV
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query_id", "dam_size", "table_size", "total_allocated_bytes_delta_pool2",
                         "total_allocated_bytes_delta_pool3", "duration"])
        writer.writerows(results)