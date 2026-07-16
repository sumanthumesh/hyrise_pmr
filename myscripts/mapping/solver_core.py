from typing import Dict, List, Optional, Set, Tuple

import pulp


def solve_column_placement(
    columns: Dict[str, Tuple[int, int]],
    memories: List[Tuple[Optional[int], int]],
) -> List[Set[str]]:
    """
    Assign each column to exactly one memory to minimize total weighted latency.

    columns : {column_name: (hotness, size)}
    memories: [(capacity, latency), ...] where capacity=None means unbounded

    Objective: minimize sum_{i in cols, j in mems} hotness_i * latency_j * x_{i,j}
    Constraints:
      - each column lands in exactly one memory
      - for every memory with a capacity, sum of chosen columns' sizes <= capacity

    Returns a list parallel to `memories`; entry j is the set of column names
    assigned to memory j.
    """
    if not memories:
        raise ValueError("At least one memory must be provided.")

    column_names = list(columns.keys())
    memory_indices = list(range(len(memories)))

    problem = pulp.LpProblem("column_placement", pulp.LpMinimize)

    x = {
        (col, mem_idx): pulp.LpVariable(f"x__{col}__m{mem_idx}", cat=pulp.LpBinary)
        for col in column_names
        for mem_idx in memory_indices
    }

    problem += pulp.lpSum(
        columns[col][0] * memories[mem_idx][1] * x[(col, mem_idx)]
        for col in column_names
        for mem_idx in memory_indices
    )

    for col in column_names:
        problem += (
            pulp.lpSum(x[(col, mem_idx)] for mem_idx in memory_indices) == 1,
            f"assign_once__{col}",
        )

    for mem_idx, (capacity, _latency) in enumerate(memories):
        if capacity is None:
            continue
        problem += (
            pulp.lpSum(columns[col][1] * x[(col, mem_idx)] for col in column_names) <= capacity,
            f"capacity__m{mem_idx}",
        )

    status = problem.solve(pulp.PULP_CBC_CMD(msg=False))
    if pulp.LpStatus[status] != "Optimal":
        raise RuntimeError(f"ILP did not solve to optimality: status={pulp.LpStatus[status]}")

    placement: List[Set[str]] = [set() for _ in memory_indices]
    for col in column_names:
        for mem_idx in memory_indices:
            if pulp.value(x[(col, mem_idx)]) > 0.5:
                placement[mem_idx].add(col)
                break

    return placement
