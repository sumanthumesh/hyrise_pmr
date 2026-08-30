# Microbenchmark set 2 — rationale

Target: TPC-H SF50 (`generate_tpch 50`), lineitem = 300 M rows, as used by the
existing `sf50_e2e_microbenchmark_cmap.sql` harness.

## Why set 1 did not work

| Q | shape | baseline | speedup | diagnosis |
|---|-------|---------:|--------:|-----------|
| 1 | scan int, COUNT       |  0.152 s | 0.765 | too short |
| 2 | scan date, COUNT      |  0.121 s | 0.730 | too short |
| 3 | scan string, COUNT    |  0.099 s | 0.708 | too short |
| 4 | join + filter         |  2.975 s | 1.260 | usable |
| 5 | group-by aggregate    | 12.849 s | 1.060 | usable |
| 6 | sort + limit          |  9.100 s | 0.219 | pathological |

Three separate problems:

1. **Q1–Q3 are single-pass scans, and a single-pass scan can never win.**
   Migration copies the column at ~34 GB/s and the query then reads it once at
   ~12 GB/s. Paying a copy to speed up one read is structurally a loss — Q1
   scans 1.796 GB in 0.152 s and pays 0.049 s to migrate it. The problem is not
   that these queries are short; it is that **nothing re-reads the bytes**.
   Every query below either aggregates, hashes, probes or sorts, so each
   migrated byte is touched more than once.

2. **No two queries shared a spine**, so a difference between Q4 and Q5 could
   be the operator, the tables, the column set, the output cardinality or the
   selectivity. Nothing was attributable.

3. **Q6's sort ran 41.5 s against a 9.1 s baseline** — a 4.6x *slowdown*. That
   is regDAM starvation: base columns occupy DAM, the sort's transient buffers
   spill to CRAM. Worth measuring deliberately, not worth leaving as an
   uncontrolled outlier.

## Design of set 2

A factorial design around one fixed spine, so each query differs from the
anchor in exactly one respect.

**Anchor (`3.sql`)**: filter lineitem to ~50 % of rows, then a scalar
`COUNT(*), SUM(l_quantity), SUM(l_extendedprice)`. No grouping, no join, no
sort. The aggregate touches the same 2.08 GB of measure columns in every query
in the set, and roughly 150 M rows reach it everywhere.

### Block A — data type axis (`1.sql`–`4.sql`)

Operator held fixed (filter + the anchor aggregate). Selectivity held at ~50 %.
**Only the type of the predicate column changes.**

| file | type | predicate column | size | selectivity |
|------|------|------------------|-----:|------------:|
| `1.sql` | integer | `l_partkey`       | 1.796 GB | ~50 % |
| `2.sql` | decimal | `l_tax`           | 0.300 GB | ~56 % |
| `3.sql` | date    | `l_shipdate`      | 1.062 GB | ~50 % |
| `4.sql` | string  | `l_shipinstruct`  | 0.302 GB | 50 % |

Because rows are held constant, **column size is the type effect** — a date is
4 bytes where an integer key is 8, and a 4-value string is dictionary-encoded
down to a fraction of either. That is exactly the quantity that sets migration
cost, so this block measures cost-per-migrated-byte by type while holding the
work done on those bytes constant. The predicate columns are deliberately
disjoint from the aggregated columns so the two effects do not overlap.

### Block B — operator axis (`3.sql`, `5.sql`–`8.sql`)

Filter and measure columns held fixed at the anchor's. **One operator added.**

| file | added operator | why it should behave differently |
|------|----------------|----------------------------------|
| `3.sql` | none (anchor)          | sequential, one pass, bandwidth-bound |
| `5.sql` | group-by, 4 groups     | per-row hashing; hash table fits in cache |
| `6.sql` | group-by, ~500 K groups| same shape as 5, but the hash table leaves cache — isolates **random access** from hashing cost |
| `7.sql` | hash join to orders    | random probes into a build side |
| `8.sql` | top-N sort             | transient-dominated; the regDAM-pressure case |

`5.sql` vs `6.sql` is the sharpest pair in the set: identical SQL except the
group key's cardinality, so any difference is attributable to hash-table
locality alone. `3.sql` vs `7.sql` isolates the join. `3.sql` vs `8.sql`
isolates transient pressure — this is the controlled version of old Q6, with
half the input rows so it should complete rather than thrash.

## Expected runtimes — estimates, calibrate before trusting

Extrapolated from set 1: scan ~11.8 GB/s (Q1), aggregate ~43 ns/row (Q5),
join ~10 ns/row (Q4).

| file | estimate | driver |
|------|---------:|--------|
| `1.sql` | 4–6 s | 150 M rows aggregated + 1.8 GB scanned |
| `2.sql` | 4–6 s | 167 M rows aggregated |
| `3.sql` | 4–6 s | 150 M rows aggregated |
| `4.sql` | 4–6 s | 150 M rows aggregated |
| `5.sql` | 6–9 s | + hashing |
| `6.sql` | 8–14 s | + random hash-table access |
| `7.sql` | 5–8 s | + join probe |
| `8.sql` | 4–8 s | sort of ~150 M rows, **or much worse if it thrashes** |

All comfortably above the ~0.15 s regime where migration overhead dominated,
and all within one order of magnitude of each other so they can share an axis.

**Run one calibration pass before the full sweep.** If everything lands above
~15 s, tighten every filter by the same factor — moving `l_shipdate`'s upper
bound to `'1994-06-30'` roughly halves all of them at once and preserves the
design, since the filter is shared. If `8.sql` blows up the way old Q6 did,
that is a result worth reporting, but shrink its filter separately so the rest
of the set stays comparable.

## What still needs doing

New mapping files. The harness pins the columns a query touches onto node 0 and
everything else onto node 1 (`mapping/map_d0_f0_q<N>_migrate.sql`), so each
query here needs its own mapping listing:

- all four blocks: `l_quantity`, `l_extendedprice` (measures)
- `1.sql`: + `l_partkey`  `2.sql`: + `l_tax`  `3.sql`: + `l_shipdate`
  `4.sql`: + `l_shipinstruct`
- `5.sql`: + `l_shipdate`, `l_returnflag`, `l_linestatus`
- `6.sql`: + `l_shipdate`, `l_suppkey`
- `7.sql`: + `l_shipdate`, `l_orderkey`, `o_orderkey`, `o_orderpriority`
- `8.sql`: + `l_shipdate`, `l_orderkey` (no `l_quantity`)
