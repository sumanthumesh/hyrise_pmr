#include "collect_read_columns.hpp"

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "storage/chunk.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise
{

namespace
{

// Resolve a (column_id relative to `input`) to the source storage-table column name by walking
// through one ReferenceSegment level if `input` is an intermediate table. This keeps the name
// matched to the storage column even if an upstream Projection/Alias renamed it in the
// intermediate schema.
// COUNT(*) is represented as a PQPColumnExpression carrying INVALID_COLUMN_ID (see
// LQPTranslator's "Resolve COUNT(*)"): it addresses no column and reads no column data, so it
// must never reach resolve_column_name() -- Chunk::get_segment()/Table::column_name() would
// assert on the out-of-range id.
bool addresses_a_column(const std::shared_ptr<const Table> &input, const ColumnID column_id)
{
    return input && column_id != INVALID_COLUMN_ID && column_id < input->column_count();
}

std::string resolve_column_name(const std::shared_ptr<const Table> &input, const ColumnID column_id)
{
    if (input->type() == TableType::References)
    {
        const auto chunk_count = input->chunk_count();
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id)
        {
            const auto chunk = input->get_chunk(chunk_id);
            if (!chunk)
            {
                continue;
            }
            const auto seg = std::dynamic_pointer_cast<const ReferenceSegment>(chunk->get_segment(column_id));
            if (!seg)
            {
                break;
            }
            return std::string{seg->referenced_table()->column_name(seg->referenced_column_id())};
        }
    }
    return std::string{input->column_name(column_id)};
}

} // namespace

ReadColumnsSnapshot collect_read_columns(const AbstractOperator &op)
{
    std::set<std::string> left_names;
    std::set<std::string> right_names;

    const auto get_input_table =
        [&](const std::shared_ptr<const AbstractOperator> &input_op) -> std::shared_ptr<const Table> {
        if (!input_op || input_op->state() != OperatorState::ExecutedAndAvailable)
        {
            return nullptr;
        }
        return input_op->get_output();
    };

    const auto left_input_table = get_input_table(op.left_input());
    const auto right_input_table = get_input_table(op.right_input());

    // Single entry point for name resolution so the INVALID_COLUMN_ID / out-of-range guard
    // cannot be bypassed by a new call site.
    const auto insert_column = [&](const std::shared_ptr<const Table> &input, const ColumnID column_id,
                                   std::set<std::string> &dest) {
        if (!addresses_a_column(input, column_id))
        {
            return;
        }
        dest.insert(resolve_column_name(input, column_id));
    };

    // Route an expression's column references into `dest`, resolving each PQPColumnExpression
    // against the given `input` table.
    const auto collect_from_expression =
        [&](const std::shared_ptr<AbstractExpression> &root, const std::shared_ptr<const Table> &input,
            std::set<std::string> &dest) {
            auto mut_root = root;
            visit_expression(mut_root, [&](const auto &sub) {
                const auto col_expr = std::dynamic_pointer_cast<PQPColumnExpression>(sub);
                if (col_expr)
                {
                    insert_column(input, col_expr->column_id, dest);
                }
                return ExpressionVisitation::VisitArguments;
            });
        };

    switch (op.type())
    {
    case OperatorType::GetTable:
        // Emitting every non-pruned column of a wide stored table would drown out downstream
        // hotness signal — leave empty and let TableScan/Join/Aggregate/Projection/Sort report
        // what they actually touched.
        break;
    case OperatorType::TableScan:
    {
        const auto &table_scan = static_cast<const TableScan &>(op);
        collect_from_expression(table_scan.predicate(), left_input_table, left_names);
        break;
    }
    case OperatorType::JoinHash:
    case OperatorType::JoinSortMerge:
    case OperatorType::JoinNestedLoop:
    case OperatorType::JoinIndex:
    case OperatorType::JoinVerification:
    {
        const auto &join = static_cast<const AbstractJoinOperator &>(op);
        const auto &primary = join.primary_predicate();
        insert_column(left_input_table, primary.column_ids.first, left_names);
        insert_column(right_input_table, primary.column_ids.second, right_names);
        for (const auto &secondary : join.secondary_predicates())
        {
            insert_column(left_input_table, secondary.column_ids.first, left_names);
            insert_column(right_input_table, secondary.column_ids.second, right_names);
        }
        break;
    }
    case OperatorType::Aggregate:
    {
        const auto &aggregate = static_cast<const AbstractAggregateOperator &>(op);
        for (const auto column_id : aggregate.groupby_column_ids())
        {
            insert_column(left_input_table, column_id, left_names);
        }
        for (const auto &agg : aggregate.aggregates())
        {
            // COUNT(*)'s argument is the INVALID_COLUMN_ID star expression; insert_column
            // filters it out, so the aggregate contributes no column read.
            if (agg->argument())
            {
                collect_from_expression(agg->argument(), left_input_table, left_names);
            }
        }
        break;
    }
    case OperatorType::Projection:
    {
        const auto &projection = static_cast<const Projection &>(op);
        for (const auto &expression : projection.expressions)
        {
            collect_from_expression(expression, left_input_table, left_names);
        }
        break;
    }
    case OperatorType::Sort:
    {
        const auto &sort = static_cast<const Sort &>(op);
        for (const auto &sort_def : sort.sort_definitions())
        {
            insert_column(left_input_table, sort_def.column, left_names);
        }
        break;
    }
    default:
        // Pass-through / structural ops (Alias, UnionAll, UnionPositions, Validate, Limit,
        // etc.) don't introduce new column reads.
        break;
    }

    return {std::vector<std::string>{left_names.begin(), left_names.end()},
            std::vector<std::string>{right_names.begin(), right_names.end()}};
}

} // namespace hyrise
