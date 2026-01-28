#!/usr/bin/env python3
"""
Analyze operator walltimes by grouping them by operator type.
"""

from typing import Dict, List, Tuple
import sys
from critical_path import parse_graph,compute_all_paths


def critical_path_operator_breakdown(filename:str) -> Dict[str, int]:
    """
    Analyze operator walltimes by grouping them by operator type.
    Returns: dict mapping operator_type -> total_walltime
    """
    operator_typemap: Dict[int, str] = {}
    
    vertices, edges, root_id = parse_graph(filename)
    paths_with_times = compute_all_paths(vertices, edges, root_id)
    critical_walltime,critical_path = paths_with_times[0]
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if parts[0] == 'V':
                vertex_id = int(parts[1])
                operator_type = parts[2]
                
                operator_typemap[vertex_id] = operator_type


    critical_operator_breakdown: Dict[str, int] = {}
    for vertex_id in critical_path:
        operator_type = operator_typemap[vertex_id]
        if operator_type not in critical_operator_breakdown.keys():
            critical_operator_breakdown[operator_type] = 0
        critical_operator_breakdown[operator_type] += vertices[vertex_id]

    return dict(sorted(critical_operator_breakdown.items(), key=lambda x: x[1], reverse=True))

def parse_graph_with_types(filename: str) -> Dict[str, int]:
    """
    Parse the graph file and return operator types and walltimes.
    Returns: dict mapping operator_type -> list of walltimes
    """
    operator_breakdown: Dict[str, int] = {}
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if parts[0] == 'V':
                vertex_id = int(parts[1])
                operator_type = parts[2]
                walltime = int(parts[3])
                
                if operator_type not in operator_breakdown:
                    operator_breakdown[operator_type] = 0
                
                operator_breakdown[operator_type] += walltime
    
    return dict(sorted(operator_breakdown.items(), key=lambda x: x[1], reverse=True))


def print_walltime_breakdown(operator_breakdown: Dict[str, int]):
    """Print walltimes grouped by operator type."""
    
    total_walltime = sum(operator_breakdown.values())
    
    for op_type, walltime in operator_breakdown.items():
        percentage = (walltime / total_walltime * 100) if total_walltime > 0 else 0
        print(f"{op_type:20s} | {walltime:12d} ns | {percentage:5.1f}%")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <graph_file>")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    try:
        operator_breakdown = parse_graph_with_types(filename)
        critical_operator_breakdown = critical_path_operator_breakdown(filename)
        print_walltime_breakdown(operator_breakdown)
        print("\nCritical Path Operator Breakdown:")
        print_walltime_breakdown(critical_operator_breakdown)

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()