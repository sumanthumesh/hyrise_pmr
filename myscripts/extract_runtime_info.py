import os
import sys
import argparse
from typing import List,Dict
import json
from unittest import result

def extract_runtime_info(input_file:str, per_op:bool):
    j = json.load(open(input_file))
    result:Dict[str,int|float|str|Dict] = {}
    result["wall_time"] = j["duration"]
    result["label"] = j["label"]
    result["query"] = j["script_file"]
    for it in j["memory_deltas"]:
        pool_name = f"pool_{it['resource_id']}_n{it['numa_node']}"
        result[f"delta_{pool_name}"] = it["total_allocated_bytes_delta"]
        result[f"allocated_{pool_name}"] = it["allocated_bytes"]
    result = dict(sorted(result.items(), key=lambda x: x[0]))
    if per_op:
        result["operators"] = {}
        for op in j["operator_memory_deltas"]:
            op_name = f"{op['operator_name']}_{op['operator_id']}"
            result["operators"][op_name] = {}
            result["operators"][op_name]["wall_time"] = op["walltime_ns"]
            for it in op["memory_deltas"]:
                pool_name = f"pool_{it['resource_id']}_n{it['numa_node']}"
                result["operators"][op_name][f"delta_{pool_name}"] = it["total_allocated_bytes_delta"]
                result["operators"][op_name][f"allocated_{pool_name}"] = it["allocated_bytes"]
            result["operators"][op_name] = dict(sorted(result["operators"][op_name].items(), key=lambda x: x[0]))
    return result


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Extract runtime infor from query_exec_info*.json files")
    parser.add_argument("input_files", nargs="+", help="Input query_exec_info*.json files")
    parser.add_argument("--per-op", action="store_true", help="Capture runtime info per operator instead of per query")
    args = parser.parse_args()

    all_headers:List[str] = []
    results:List[Dict] = []

    for input_file in args.input_files:
        if not os.path.exists(input_file):
            print(f"Input file {input_file} does not exist", file=sys.stderr)
            sys.exit(1)
        result = extract_runtime_info(input_file, args.per_op)
        results.append(result)
        for key in result.keys():
            if key not in all_headers and key != "operators":
                all_headers.append(key)
    
    print(",".join(all_headers))
    for result in results:
        row = []
        for key in all_headers:
            row.append(str(result.get(key, "")))
        print(",".join(row))
        
        

