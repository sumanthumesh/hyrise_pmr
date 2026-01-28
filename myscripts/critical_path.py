#!/usr/bin/env python3
"""
Compute the critical path in a directed acyclic graph (DAG) based on walltime.
The critical path is the longest path from any leaf to the root node.
"""

from typing import Dict, List, Tuple, Set
import sys


def parse_graph(filename: str) -> Tuple[Dict[int, int], Dict[int, List[int]], int]:
    """
    Parse the graph file and return:
    - vertices: dict mapping vertex_id -> walltime
    - edges: dict mapping src_id -> list of dest_ids
    - root_id: the vertex with the largest ID
    """
    vertices: Dict[int, int] = {}
    edges: Dict[int, List[int]] = {}
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if parts[0] == 'V':
                vertex_id = int(parts[1])
                walltime = int(parts[3])
                vertices[vertex_id] = walltime
                if vertex_id not in edges:
                    edges[vertex_id] = []
            elif parts[0] == 'E':
                src_id = int(parts[1])
                dest_id = int(parts[2])
                if src_id not in edges:
                    edges[src_id] = []
                edges[src_id].append(dest_id)
    
    root_id = max(vertices.keys())
    return vertices, edges, root_id


def compute_all_paths(vertices: Dict[int, int], edges: Dict[int, List[int]], root_id: int) -> List[Tuple[int, List[int]]]:
    """
    Find all paths from root to source nodes, calculate their walltimes, and return them sorted by walltime.
    Returns: list of (total_walltime, path_as_list_of_vertex_ids)
    """
    # Build reverse edges for traversal from root backwards
    reverse_edges: Dict[int, List[int]] = {}
    for src in edges:
        for dest in edges[src]:
            if dest not in reverse_edges:
                reverse_edges[dest] = []
            reverse_edges[dest].append(src)
    
    all_paths: List[List[int]] = []
    
    def find_all_paths(node_id: int, current_path: List[int]):
        """
        DFS to find all paths from root backwards to source nodes.
        """
        current_path.append(node_id)
        
        # Base case: if this node has no incoming edges, it's a source node
        if node_id not in reverse_edges or len(reverse_edges[node_id]) == 0:
            # Found a complete path from source to root
            all_paths.append(current_path[:])  # Save a copy of the path
        else:
            # Recursive case: explore all parents
            for parent_id in reverse_edges[node_id]:
                find_all_paths(parent_id, current_path)
        
        current_path.pop()
    
    # Find all paths starting from root
    find_all_paths(root_id, [])
    
    # Calculate walltime for each path
    paths_with_times: List[Tuple[int, List[int]]] = []
    
    for path in all_paths:
        path_walltime = sum(vertices[node_id] for node_id in path)
        paths_with_times.append((path_walltime, path))
    
    # Sort by walltime in descending order
    paths_with_times.sort(reverse=True, key=lambda x: x[0])
    
    return paths_with_times

def print_all_paths(vertices: Dict[int, int], paths_with_times: List[Tuple[int, List[int]]]):
    """Pretty print all paths with their walltimes."""
    
    for i, (total_walltime, path) in enumerate(paths_with_times):
        path_time = sum(vertices[node_id] for node_id in path)
        path_str = " -> ".join(str(node_id) for node_id in path)
        print(f"Path {i+1:2d}: {total_walltime:10d} ns | Path: {path_str}")

def print_critical_path(vertices: Dict[int, int], critical_path: List[int], total_walltime: int):
    """Pretty print the critical path with operator names and walltimes."""
    print(f"\n{'='*80}")
    print(f"CRITICAL PATH (Total Walltime: {total_walltime} nanoseconds)")
    print(f"{'='*80}\n")
    
    cumulative_time = 0
    for i, node_id in enumerate(critical_path):
        walltime = vertices[node_id]
        cumulative_time += walltime
        print(f"{i+1:2d}. Operator ID {node_id:2d} | Walltime: {walltime:10d} ns | Cumulative: {cumulative_time:10d} ns")
    
    print(f"\n{'='*80}")
    print(f"Total Critical Path Walltime: {total_walltime} nanoseconds")
    print(f"Path Length: {len(critical_path)} operators")
    print(f"{'='*80}\n")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <graph_file>")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    try:
        vertices, edges, root_id = parse_graph(filename)
        print(f"\nLoaded graph with {len(vertices)} vertices and {sum(len(v) for v in edges.values())} edges")
        print(f"Root node ID: {root_id}")
        
        paths_with_times = compute_all_paths(vertices, edges, root_id)
        print_all_paths(vertices, paths_with_times)
        print_critical_path(vertices, paths_with_times[0][1], paths_with_times[0][0])
        
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()