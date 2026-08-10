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

std::vector<std::string> collect_read_columns(const AbstractOperator &op)
{
    std::set<std::string> names;

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

    const auto collect_from_expression =
        [&](const std::shared_ptr<AbstractExpression> &root, const std::shared_ptr<const Table> &input) {
            auto mut_root = root;
            visit_expression(mut_root, [&](const auto &sub) {
                const auto col_expr = std::dynamic_pointer_cast<PQPColumnExpression>(sub);
                if (col_expr && input)
                {
                    names.insert(resolve_column_name(input, col_expr->column_id));
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
        collect_from_expression(table_scan.predicate(), left_input_table);
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
        if (left_input_table)
        {
            names.insert(resolve_column_name(left_input_table, primary.column_ids.first));
        }
        if (right_input_table)
        {
            names.insert(resolve_column_name(right_input_table, primary.column_ids.second));
        }
        for (const auto &secondary : join.secondary_predicates())
        {
            if (left_input_table)
            {
                names.insert(resolve_column_name(left_input_table, secondary.column_ids.first));
            }
            if (right_input_table)
            {
                names.insert(resolve_column_name(right_input_table, secondary.column_ids.second));
            }
        }
        break;
    }
    case OperatorType::Aggregate:
    {
        const auto &aggregate = static_cast<const AbstractAggregateOperator &>(op);
        if (left_input_table)
        {
            for (const auto column_id : aggregate.groupby_column_ids())
            {
                names.insert(resolve_column_name(left_input_table, column_id));
            }
            for (const auto &agg : aggregate.aggregates())
            {
                if (agg->argument())
                {
                    collect_from_expression(agg->argument(), left_input_table);
                }
            }
        }
        break;
    }
    case OperatorType::Projection:
    {
        const auto &projection = static_cast<const Projection &>(op);
        if (left_input_table)
        {
            for (const auto &expression : projection.expressions)
            {
                collect_from_expression(expression, left_input_table);
            }
        }
        break;
    }
    case OperatorType::Sort:
    {
        const auto &sort = static_cast<const Sort &>(op);
        if (left_input_table)
        {
            for (const auto &sort_def : sort.sort_definitions())
            {
                names.insert(resolve_column_name(left_input_table, sort_def.column));
            }
        }
        break;
    }
    default:
        // Pass-through / structural ops (Alias, UnionAll, UnionPositions, Validate, Limit,
        // etc.) don't introduce new column reads.
        break;
    }

    return {names.begin(), names.end()};
}

} // namespace hyrise
