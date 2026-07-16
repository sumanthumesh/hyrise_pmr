import os
import sys
import argparse
import json

def parser_hotness_file(hotness_file_path):
    with open(hotness_file_path, 'r') as f:
        hotness_raw = json.load(f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run the column placement solver"
    )

    parser.add_argument("-h","--hotness-file", required=True, help="Path to the hotness json file")
    parser.add_argument("-s","--size-file", required=True, help="Path to the size json file")
    parser.add_argument("-c","--mem-capacities", required=True, help="comma-separated list of memory capacities (use 'None' for unbounded)")
    parser.add_argument("-l","--mem-latencies", required=True, help="comma-separated list of memory latencies (must match the number of capacities)")
    parser.add_argument("-o","--output-file", required=True, help="Path to the output json file")

    args = parser.parse_args()

