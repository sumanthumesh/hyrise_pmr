#include "visualization/pqp_visualizer.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <locale>
#include <memory>
#include <ratio>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

// False positive with GCC, finding accesses to unitialized memory in adjacency_list.hpp
// (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92194).
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <boost/graph/adjacency_list.hpp>
#pragma GCC diagnostic pop

#include "nlohmann/json.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "storage/chunk.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/format_duration.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace hyrise
{

PQPVisualizer::PQPVisualizer() = default;

PQPVisualizer::PQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void PQPVisualizer::visualize(const std::vector<std::shared_ptr<AbstractOperator>> &plans, const std::string &img_filename)
{
    AbstractVisualizer::visualize(plans, img_filename);
    // auto txt_filename = img_filename;
    // const auto last_dot = txt_filename.find_last_of('.');
    // if (last_dot != std::string::npos)
    // {
    //     txt_filename = txt_filename.substr(0, last_dot);
    // }
    // txt_filename += ".graph";
    // export_as_graph_text(plans, txt_filename);

    // auto json_filename = img_filename;
    // const auto last_dot_json = json_filename.find_last_of('.');
    // if (last_dot_json != std::string::npos)
    // {
    //     json_filename = json_filename.substr(0, last_dot_json);
    // }
    // json_filename += ".graph.json";
    // export_as_graph_json(plans, json_filename);

    auto tree_filename = img_filename;
    const auto last_dot_tree = tree_filename.find_last_of('.');
    if (last_dot_tree != std::string::npos)
    {
        tree_filename = tree_filename.substr(0, last_dot_tree);
    }
    tree_filename += ".tree.json";
    export_as_hierarchical_json(plans, tree_filename);
}

void PQPVisualizer::_build_graph(const std::vector<std::shared_ptr<AbstractOperator>> &plans)
{
    std::unordered_set<std::shared_ptr<const AbstractOperator>> visualized_ops;

    for (const auto &plan : plans)
    {
        _build_subtree(plan, visualized_ops);
    }

    {
        // Print the "Total by operator" box using graphviz's record type. Using HTML labels would be slightly nicer, but
        // boost always encloses the label in quotes, which breaks them.
        auto operator_breakdown_stream = std::stringstream{};
        operator_breakdown_stream << "{Total by operator|{";

        auto sorted_duration_by_operator_name = std::vector<std::pair<std::string, std::chrono::nanoseconds>>{
            _duration_by_operator_name.begin(), _duration_by_operator_name.end()};
        std::ranges::sort(sorted_duration_by_operator_name, [](const auto &lhs, const auto &rhs)
                          { return lhs.second > rhs.second; });

        // Print first column (operator name).
        for (const auto &[operator_name, _] : sorted_duration_by_operator_name)
        {
            operator_breakdown_stream << " " << operator_name << " \\r";
        }
        operator_breakdown_stream << "total\\r";

        // Print second column (operator duration) and track total duration.
        operator_breakdown_stream << "|";
        auto total_nanoseconds = std::chrono::nanoseconds{};
        for (const auto &[_, nanoseconds] : sorted_duration_by_operator_name)
        {
            operator_breakdown_stream << " " << format_duration(nanoseconds) << " \\l";
            total_nanoseconds += nanoseconds;
        }
        operator_breakdown_stream << " " << format_duration(total_nanoseconds) << " \\l";

        // Print third column (relative operator duration)
        operator_breakdown_stream << "|";
        for (const auto &[_, nanoseconds] : sorted_duration_by_operator_name)
        {
            operator_breakdown_stream << std::round(std::chrono::duration<double, std::nano>{nanoseconds} /
                                                    std::chrono::duration<double, std::nano>{total_nanoseconds} * 100)
                                      << " %\\l";
        }
        operator_breakdown_stream << " \\l";

        operator_breakdown_stream << "}}";

        VizVertexInfo vertex_info = _default_vertex;
        vertex_info.shape = "record";
        vertex_info.label = operator_breakdown_stream.str();

        boost::add_vertex(vertex_info, _graph);
    }
}

void PQPVisualizer::_build_subtree(const std::shared_ptr<const AbstractOperator> &op,
                                   std::unordered_set<std::shared_ptr<const AbstractOperator>> &visualized_ops)
{
    // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
    if (visualized_ops.contains(op))
    {
        return;
    }
    visualized_ops.insert(op);

    _add_operator(op);

    if (op->left_input())
    {
        auto left = op->left_input();
        _build_subtree(left, visualized_ops);
        _build_dataflow(left, op, InputSide::Left);
    }

    if (op->right_input())
    {
        auto right = op->right_input();
        _build_subtree(right, visualized_ops);
        _build_dataflow(right, op, InputSide::Right);
    }

    switch (op->type())
    {
    case OperatorType::Projection:
    {
        const auto projection = std::dynamic_pointer_cast<const Projection>(op);
        for (const auto &expression : projection->expressions)
        {
            _visualize_subqueries(op, expression, visualized_ops);
        }
    }
    break;

    case OperatorType::TableScan:
    {
        const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
        _visualize_subqueries(op, table_scan->predicate(), visualized_ops);
    }
    break;

    case OperatorType::Limit:
    {
        const auto limit = std::dynamic_pointer_cast<const Limit>(op);
        _visualize_subqueries(op, limit->row_count_expression(), visualized_ops);
    }
    break;

    default:
    {
    } // OperatorType has no expressions
    }
}

void PQPVisualizer::_visualize_subqueries(const std::shared_ptr<const AbstractOperator> &op,
                                          const std::shared_ptr<AbstractExpression> &expression,
                                          std::unordered_set<std::shared_ptr<const AbstractOperator>> &visualized_ops)
{
    visit_expression(expression, [&](const auto &sub_expression)
                     {
    const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
    if (!pqp_subquery_expression) {
      return ExpressionVisitation::VisitArguments;
    }

    _build_subtree(pqp_subquery_expression->pqp, visualized_ops);

    auto edge_info = _default_edge;
    auto correlated_str = std::string(pqp_subquery_expression->is_correlated() ? "correlated" : "uncorrelated");
    edge_info.label = correlated_str + " subquery";
    edge_info.style = "dashed";
    _add_edge(pqp_subquery_expression->pqp, op, edge_info);

    return ExpressionVisitation::VisitArguments; });
}

void PQPVisualizer::_build_dataflow(const std::shared_ptr<const AbstractOperator> &source_node,
                                    const std::shared_ptr<const AbstractOperator> &target_node, const InputSide side)
{
    VizEdgeInfo info = _default_edge;

    const auto &performance_data = *source_node->performance_data;
    if (source_node->executed() && performance_data.has_output)
    {
        auto stream = std::stringstream{};

        // Use a copy of the stream's default locale with thousands separators: Dynamically allocated raw pointers should
        // be avoided whenever possible. Unfortunately, std::locale stores pointers to the facets and does internal
        // reference counting. std::locale's destructor destructs the locale and the facets whose reference count becomes
        // zero. This forces us to use a dynamically allocated raw pointer here.
        const auto &separate_thousands_locale = std::locale(stream.getloc(), new SeparateThousandsFacet);
        stream.imbue(separate_thousands_locale);

        stream << performance_data.output_row_count << " row(s)/";
        stream << performance_data.output_chunk_count << " chunk(s)";
        info.label = stream.str();
    }

    info.pen_width = static_cast<double>(performance_data.output_row_count);
    if (target_node->right_input() != nullptr)
    {
        info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
    }

    _add_edge(source_node, target_node, info);
}

void PQPVisualizer::_add_operator(const std::shared_ptr<const AbstractOperator> &op)
{
    VizVertexInfo info = _default_vertex;
    auto label = std::to_string(op->operator_id) + "\n";
    label += op->description(DescriptionMode::MultiLine);

    const auto &performance_data = *op->performance_data;
    if (op->executed())
    {
        auto total = performance_data.walltime;
        label += "\n\n" + format_duration(total);
        info.pen_width = static_cast<double>(total.count());

        auto operator_performance_data_stream = std::stringstream{};
        performance_data.output_to_stream(operator_performance_data_stream, DescriptionMode::MultiLine);
        info.tooltip = operator_performance_data_stream.str();
    }
    else
    {
        info.pen_width = 1.0;
    }

    _duration_by_operator_name[op->name()] += performance_data.walltime;

    info.label = label;
    _add_vertex(op, info);
}

void PQPVisualizer::export_as_graph_text(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                                         const std::string &text_filename)
{
    std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> nodes_map;
    std::vector<PQPEdge> edges_list;

    _collect_graph_info(plans, nodes_map, edges_list);

    auto file = std::ofstream(text_filename);
    Assert(file.is_open(), "Failed to open file for writing: " + text_filename);

    // Write nodes section
    for (const auto &[op_id, node_info] : nodes_map)
    {
        const auto &[operator_type, walltime] = node_info;
        file << "V," << op_id << "," << operator_type << "," << walltime.count() << "\n";
    }

    // Write edges section
    for (const auto &edge : edges_list)
    {
        file << "E," << edge.src_operator_id << "," << edge.dst_operator_id << "," << edge.row_count << "\n";
    }

    file.close();
}

void PQPVisualizer::export_as_graph_json(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                                         const std::string &json_filename)
{
    std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> nodes_map;
    std::vector<PQPEdge> edges_list;

    _collect_graph_info(plans, nodes_map, edges_list);

    auto nodes_json = nlohmann::json::array();
    for (const auto &[op_id, node_info] : nodes_map)
    {
        const auto &[operator_type, walltime] = node_info;
        nodes_json.push_back({
            {"id", op_id},
            {"name", operator_type},
            {"walltime_ns", walltime.count()},
        });
    }

    auto edges_json = nlohmann::json::array();
    for (const auto &edge : edges_list)
    {
        edges_json.push_back({
            {"src", edge.src_operator_id},
            {"dst", edge.dst_operator_id},
            {"rows", edge.row_count},
        });
    }

    auto graph_json = nlohmann::json{
        {"nodes", nodes_json},
        {"edges", edges_json},
    };

    auto file = std::ofstream(json_filename);
    Assert(file.is_open(), "Failed to open file for writing: " + json_filename);
    file << graph_json.dump(2) << "\n";
}

void PQPVisualizer::export_as_hierarchical_json(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                                                const std::string &json_filename)
{
    _stored_table_names.clear();
    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_ops;
    auto trees_json = nlohmann::json::array();
    for (const auto &plan : plans)
    {
        trees_json.push_back(_build_hierarchical_subtree(plan, visited_ops));
    }

    // If a single plan, emit its root object at the top level; otherwise emit the array.
    auto out_json = (plans.size() == 1) ? trees_json.front() : nlohmann::json{trees_json};

    auto file = std::ofstream(json_filename);
    Assert(file.is_open(), "Failed to open file for writing: " + json_filename);
    file << out_json.dump(2) << "\n";
}

// Returns the "N row(s)/M chunk(s)" label used on dataflow edges, or "" if the source op
// hasn't run yet.
static std::string dataflow_label(const std::shared_ptr<const AbstractOperator> &source_node)
{
    const auto &performance_data = *source_node->performance_data;
    if (!source_node->executed() || !performance_data.has_output)
    {
        return {};
    }
    auto stream = std::stringstream{};
    const auto &separate_thousands_locale = std::locale(stream.getloc(), new SeparateThousandsFacet);
    stream.imbue(separate_thousands_locale);
    stream << performance_data.output_row_count << " row(s)/"
           << performance_data.output_chunk_count << " chunk(s)";
    return stream.str();
}

// Live-lookup implementation was removed — column info is now snapshotted onto
// performance_data->read_columns_snapshot during AbstractOperator::execute() while the input
// tables are still available. See operators/collect_read_columns.hpp.
#if 0
static std::pair<std::string, std::string> resolve_column(const std::shared_ptr<const Table> &input, const ColumnID column_id,
                                std::unordered_map<const void *, std::string> &stored_table_names)
{
    // Lazy cache lookup helper.
    auto name_of = [&](const std::shared_ptr<const Table> &table) -> std::string {
        const auto key = static_cast<const void *>(table.get());
        const auto it = stored_table_names.find(key);
        if (it != stored_table_names.end())
        {
            return it->second;
        }
        for (const auto &[name, stored_table] : Hyrise::get().storage_manager.tables())
        {
            if (stored_table.get() == table.get())
            {
                stored_table_names[key] = name;
                return name;
            }
        }
        // Not found in StorageManager (e.g., a purely intermediate table).
        stored_table_names[key] = std::string{"<unknown>"};
        return "<unknown>";
    };

    if (input->type() == TableType::References)
    {
        // Find a non-null chunk to inspect; chunk 0 may be pruned.
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
                break;  // fall through to the stored-table branch
            }
            const auto referenced_table = seg->referenced_table();
            return {name_of(referenced_table),
                    std::string{referenced_table->column_name(seg->referenced_column_id())}};
        }
    }
    return {name_of(input), std::string{input->column_name(column_id)}};
}

#endif  // end of legacy live-lookup fallback

nlohmann::json PQPVisualizer::_collect_columns(const std::shared_ptr<const AbstractOperator> &op)
{
    // Kept for backward compat: the combined list (left + right, deduped).
    std::set<std::string> merged{op->performance_data->left_read_columns.begin(),
                                 op->performance_data->left_read_columns.end()};
    merged.insert(op->performance_data->right_read_columns.begin(),
                  op->performance_data->right_read_columns.end());
    return std::vector<std::string>{merged.begin(), merged.end()};
}

#if 0  // Retired live-lookup implementation kept for reference.
nlohmann::json PQPVisualizer::_collect_columns_LEGACY(const std::shared_ptr<const AbstractOperator> &op)
{
    // Deduplicated collection keyed by (table, column) so an operator that references the same
    // column multiple times (e.g. l_shipdate BETWEEN X AND Y) counts once.
    std::set<std::pair<std::string, std::string>> refs;

    // Walk an expression tree, pushing every PQPColumnExpression's column against `input`.
    const auto collect_from_expression =
        [&](const std::shared_ptr<AbstractExpression> &root, const std::shared_ptr<const Table> &input) {
            auto mut_root = root;  // visit_expression wants a non-const lvalue.
            visit_expression(mut_root, [&](const auto &sub) {
                const auto col_expr = std::dynamic_pointer_cast<PQPColumnExpression>(sub);
                if (col_expr && input)
                {
                    refs.insert(resolve_column(input, col_expr->column_id, _stored_table_names));
                }
                return ExpressionVisitation::VisitArguments;
            });
        };

    // Fetching an input operator's output table only works when it's still ExecutedAndAvailable.
    // After a full query run only the root retains its output; every intermediate op is cleared
    // to save memory. Guard so `visualize pqp` on a completed pipeline degrades gracefully to
    // empty column lists instead of crashing.
    const auto safe_output = [](const std::shared_ptr<const AbstractOperator> &input_op) {
        if (!input_op || input_op->state() != OperatorState::ExecutedAndAvailable)
        {
            return std::shared_ptr<const Table>{};
        }
        return input_op->get_output();
    };
    const auto left_input_table = safe_output(op->left_input());
    const auto right_input_table = safe_output(op->right_input());

    switch (op->type())
    {
    case OperatorType::GetTable:
    {
        // A GetTable materializes all non-pruned columns of the stored table. Attributing every
        // column of a wide table to GetTable's row count would drown out downstream signals, so
        // deliberately emit an empty list here — downstream TableScan/Join/Aggregate/Projection
        // ops name what they actually read.
        break;
    }
    case OperatorType::TableScan:
    {
        const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
        collect_from_expression(table_scan->predicate(), left_input_table);
        break;
    }
    case OperatorType::JoinHash:
    case OperatorType::JoinSortMerge:
    case OperatorType::JoinNestedLoop:
    case OperatorType::JoinIndex:
    case OperatorType::JoinVerification:
    {
        const auto join = std::dynamic_pointer_cast<const AbstractJoinOperator>(op);
        if (join)
        {
            const auto &primary = join->primary_predicate();
            if (left_input_table)
            {
                refs.insert(resolve_column(left_input_table, primary.column_ids.first, _stored_table_names));
            }
            if (right_input_table)
            {
                refs.insert(resolve_column(right_input_table, primary.column_ids.second, _stored_table_names));
            }
            for (const auto &secondary : join->secondary_predicates())
            {
                if (left_input_table)
                {
                    refs.insert(resolve_column(left_input_table, secondary.column_ids.first, _stored_table_names));
                }
                if (right_input_table)
                {
                    refs.insert(resolve_column(right_input_table, secondary.column_ids.second, _stored_table_names));
                }
            }
        }
        break;
    }
    case OperatorType::Aggregate:
    {
        const auto aggregate = std::dynamic_pointer_cast<const AbstractAggregateOperator>(op);
        if (aggregate && left_input_table)
        {
            for (const auto column_id : aggregate->groupby_column_ids())
            {
                refs.insert(resolve_column(left_input_table, column_id, _stored_table_names));
            }
            for (const auto &agg : aggregate->aggregates())
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
        const auto projection = std::dynamic_pointer_cast<const Projection>(op);
        if (projection && left_input_table)
        {
            for (const auto &expression : projection->expressions)
            {
                collect_from_expression(expression, left_input_table);
            }
        }
        break;
    }
    case OperatorType::Sort:
    {
        const auto sort = std::dynamic_pointer_cast<const Sort>(op);
        if (sort && left_input_table)
        {
            for (const auto &sort_def : sort->sort_definitions())
            {
                refs.insert(resolve_column(left_input_table, sort_def.column, _stored_table_names));
            }
        }
        break;
    }
    default:
        // Pass-through / structural operators (Alias, UnionAll, UnionPositions, Validate, Limit,
        // GetTable already handled, etc.) don't read new columns.
        break;
    }

    auto out = nlohmann::json::array();
    for (const auto &[table_name, column_name] : refs)
    {
        out.push_back({{"table_name", table_name}, {"column_name", column_name}});
    }
    return out;
}
#endif  // end of retired live-lookup _collect_columns

namespace
{
// Best-effort stringification of an AllTypeVariant for structured-predicate emission.
// Falls back to "<unstringifiable>" so a rare-type predicate never breaks JSON export.
std::string variant_to_string(const AllTypeVariant &value)
{
    try
    {
        auto oss = std::ostringstream{};
        oss << value;
        return oss.str();
    }
    catch (...)
    {
        return std::string{"<unstringifiable>"};
    }
}

std::string predicate_condition_string(PredicateCondition condition)
{
    auto oss = std::ostringstream{};
    oss << condition;
    return oss.str();
}

// Extract the value payload from an expression if it is a ValueExpression; empty string otherwise.
std::string value_expression_string(const std::shared_ptr<AbstractExpression> &expression)
{
    if (const auto value_expr = std::dynamic_pointer_cast<const ValueExpression>(expression))
    {
        return variant_to_string(value_expr->value);
    }
    // Anything else (subquery, another column, arithmetic) — fall back to as_column_name()
    // so the JSON still carries something recognizable.
    return expression ? expression->as_column_name() : std::string{};
}

// Fills node["predicate"] with a structured {column, op, [value|lower|upper]} block
// for the three predicate flavors TableScan accepts.
void emit_structured_predicate(nlohmann::json &node, const std::shared_ptr<AbstractExpression> &predicate)
{
    if (!predicate) return;

    if (const auto binary = std::dynamic_pointer_cast<const BinaryPredicateExpression>(predicate))
    {
        node["predicate"] = {
            {"kind", "binary"},
            {"column", binary->left_operand()->as_column_name()},
            {"op", predicate_condition_string(binary->predicate_condition)},
            {"value", value_expression_string(binary->right_operand())},
        };
        return;
    }
    if (const auto between = std::dynamic_pointer_cast<const BetweenExpression>(predicate))
    {
        node["predicate"] = {
            {"kind", "between"},
            {"column", between->operand()->as_column_name()},
            {"op", predicate_condition_string(between->predicate_condition)},
            {"lower", value_expression_string(between->lower_bound())},
            {"upper", value_expression_string(between->upper_bound())},
        };
        return;
    }
    if (const auto is_null = std::dynamic_pointer_cast<const IsNullExpression>(predicate))
    {
        node["predicate"] = {
            {"kind", "is_null"},
            {"column", is_null->operand()->as_column_name()},
            {"op", predicate_condition_string(is_null->predicate_condition)},
        };
        return;
    }
    // Complex predicate (subquery, expression eval) — record kind only, no structural fields.
    node["predicate"] = {{"kind", "complex"}};
}
} // namespace

nlohmann::json PQPVisualizer::_build_hierarchical_subtree(
    const std::shared_ptr<const AbstractOperator> &op,
    std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops)
{
    // Diamond-shaped PQPs: shared subtrees are emitted only once. Prior versions returned
    // an empty {} placeholder here, which showed up in the JSON as a nameless "?" node.
    // Emit a minimal stub that preserves identity so consumers can join back to the
    // full node emitted elsewhere in the tree.
    if (visited_ops.contains(op))
    {
        return nlohmann::json{
            {"id", op->operator_id},
            {"name", std::string{op->name()}},
            {"shared_ref", true},
        };
    }
    visited_ops.insert(op);

    auto node = nlohmann::json::object();
    node["id"] = op->operator_id;
    node["name"] = std::string{op->name()};
    node["walltime_ns"] = op->performance_data->walltime.count();
    node["description"] = op->description();
    node["columns_left"] = op->performance_data->left_read_columns;
    node["columns_right"] = op->performance_data->right_read_columns;
    node["columns"] = _collect_columns(op);

    if (op->left_input())
    {
        const auto left = op->left_input();
        node["Leftchildren"] = _build_hierarchical_subtree(left, visited_ops);
        node["LeftCard"] = dataflow_label(left);
    }

    if (op->right_input())
    {
        const auto right = op->right_input();
        node["Rightchildren"] = _build_hierarchical_subtree(right, visited_ops);
        node["RightCard"] = dataflow_label(right);
    }

    // Per-operator-type structured detail. Emitted as extra keys on the same node so
    // consumers can read them alongside the generic fields.
    switch (op->type())
    {
    case OperatorType::Projection:
        node["Ops"] = "projection";
        break;
    case OperatorType::TableScan:
    {
        node["Ops"] = "TableScan";
        // Reference-vs-value nature of the input flips the TableScan allocation regime
        // (base scan builds only `matches`; reference scan adds filtered_pos_lists +
        // chunk_offsets_by_chunk_id). Captured on performance_data during execute().
        node["is_reference_input"] = static_cast<int>(op->performance_data->left_input_is_reference);
        const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
        if (table_scan)
        {
            emit_structured_predicate(node, table_scan->predicate());
        }
        break;
    }
    case OperatorType::Limit:
        node["Ops"] = "Limit";
        break;
    case OperatorType::GetTable:
    {
        // GetTable's own columns_left/right/columns are empty (it reads nothing itself).
        // Emit the surviving-column list here so downstream analysis can map the query's
        // referenced columns back to their originating base table without needing to
        // parse the "pruned: X/Y column(s)" hint out of description.
        const auto get_table = std::dynamic_pointer_cast<const GetTable>(op);
        if (get_table)
        {
            const auto &table = *Hyrise::get().storage_manager.get_table(get_table->table_name());
            const auto &pruned = get_table->pruned_column_ids();
            auto used = std::vector<std::string>{};
            used.reserve(table.column_count());
            auto pruned_it = pruned.begin();
            for (auto column_id = ColumnID{0}; column_id < table.column_count(); ++column_id)
            {
                if (pruned_it != pruned.end() && *pruned_it == column_id)
                {
                    ++pruned_it;
                    continue;
                }
                used.push_back(table.column_name(column_id));
            }
            node["used_columns"] = used;
            node["table_name"] = get_table->table_name();
        }
        break;
    }
    case OperatorType::JoinHash:
    {
        // Which side is the hash-table build side. Build side dominates JoinHash temp
        // memory (~24 B per row for the hash table), so this decides the memory model.
        const auto perf = dynamic_cast<const JoinHash::PerformanceData *>(op->performance_data.get());
        if (perf)
        {
            node["build_side"] = perf->left_input_is_build_side ? "left" : "right";
        }
        break;
    }
    case OperatorType::Aggregate:
    {
        // Split the AggregateHash node's inputs into group-by columns and aggregate
        // expressions so downstream analysis can size the hash table
        // (#groups × (key_bytes + aggregate_state_bytes)) properly.
        const auto agg = std::dynamic_pointer_cast<const AbstractAggregateOperator>(op);
        if (agg)
        {
            auto agg_exprs = std::vector<std::string>{};
            agg_exprs.reserve(agg->aggregates().size());
            for (const auto &expression : agg->aggregates())
            {
                agg_exprs.push_back(expression->as_column_name());
            }
            node["aggregate_expressions"] = agg_exprs;
            // Raw group-by column IDs into the aggregate's INPUT table. Resolving to
            // names requires the input's column_definitions, which may have been cleared
            // by deregister_consumer() before this runs; consumers can join IDs back to
            // names via the upstream GetTable's used_columns list.
            auto gb_ids = std::vector<uint32_t>{};
            gb_ids.reserve(agg->groupby_column_ids().size());
            for (const auto column_id : agg->groupby_column_ids())
            {
                gb_ids.push_back(static_cast<uint32_t>(column_id));
            }
            node["groupby_column_ids"] = gb_ids;
        }
        break;
    }
    default:
        break;
    }

    return node;
}

void PQPVisualizer::_collect_graph_info(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                                        std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> &nodes_map,
                                        std::vector<PQPEdge> &edges_list)
{
    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_ops;

    for (const auto &plan : plans)
    {
        _collect_subtree_info(plan, visited_ops, nodes_map, edges_list);
    }
}

void PQPVisualizer::_collect_subtree_info(const std::shared_ptr<const AbstractOperator> &op,
                                          std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops,
                                          std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> &nodes_map,
                                          std::vector<PQPEdge> &edges_list)
{
    // Avoid processing operators redundantly in diamond shaped PQPs
    if (visited_ops.contains(op))
    {
        return;
    }
    visited_ops.insert(op);

    // Add this operator as a node
    nodes_map[op->operator_id] = {op->name(), op->performance_data->walltime};

    // Process left input and add edge
    if (op->left_input())
    {
        auto left = op->left_input();
        _collect_subtree_info(left, visited_ops, nodes_map, edges_list);
        const auto &left_perf = *left->performance_data;
        const auto rows =
            (left->executed() && left_perf.has_output) ? left_perf.output_row_count : size_t{0};
        edges_list.push_back({left->operator_id, op->operator_id, rows});
    }

    // Process right input and add edge
    if (op->right_input())
    {
        auto right = op->right_input();
        _collect_subtree_info(right, visited_ops, nodes_map, edges_list);
        const auto &right_perf = *right->performance_data;
        const auto rows =
            (right->executed() && right_perf.has_output) ? right_perf.output_row_count : size_t{0};
        edges_list.push_back({right->operator_id, op->operator_id, rows});
    }

    // Handle subqueries
    switch (op->type())
    {
    case OperatorType::Projection:
    {
        const auto projection = std::dynamic_pointer_cast<const Projection>(op);
        for (auto expression : projection->expressions)
        {
            // Cast away const to use visit_expression (we only read, don't modify)
            auto mutable_expression = const_cast<std::shared_ptr<AbstractExpression> &>(expression);
            visit_expression(mutable_expression, [&](const auto &sub_expression)
                             {
                const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
                if (!pqp_subquery_expression) {
                  return ExpressionVisitation::VisitArguments;
                }
                _collect_subtree_info(pqp_subquery_expression->pqp, visited_ops, nodes_map, edges_list);
                { const auto &perf = *pqp_subquery_expression->pqp->performance_data; const auto rows = (pqp_subquery_expression->pqp->executed() && perf.has_output) ? perf.output_row_count : size_t{0}; edges_list.push_back({pqp_subquery_expression->pqp->operator_id, op->operator_id, rows}); }
                return ExpressionVisitation::VisitArguments; });
        }
    }
    break;

    case OperatorType::TableScan:
    {
        const auto table_scan = std::dynamic_pointer_cast<const TableScan>(op);
        auto predicate = table_scan->predicate();
        auto mutable_predicate = const_cast<std::shared_ptr<AbstractExpression> &>(predicate);
        visit_expression(mutable_predicate, [&](const auto &sub_expression)
                         {
            const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
            if (!pqp_subquery_expression) {
              return ExpressionVisitation::VisitArguments;
            }
            _collect_subtree_info(pqp_subquery_expression->pqp, visited_ops, nodes_map, edges_list);
            { const auto &perf = *pqp_subquery_expression->pqp->performance_data; const auto rows = (pqp_subquery_expression->pqp->executed() && perf.has_output) ? perf.output_row_count : size_t{0}; edges_list.push_back({pqp_subquery_expression->pqp->operator_id, op->operator_id, rows}); }
            return ExpressionVisitation::VisitArguments; });
    }
    break;

    case OperatorType::Limit:
    {
        const auto limit = std::dynamic_pointer_cast<const Limit>(op);
        auto row_count_expr = limit->row_count_expression();
        auto mutable_row_count_expr = const_cast<std::shared_ptr<AbstractExpression> &>(row_count_expr);
        visit_expression(mutable_row_count_expr, [&](const auto &sub_expression)
                         {
            const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
            if (!pqp_subquery_expression) {
              return ExpressionVisitation::VisitArguments;
            }
            _collect_subtree_info(pqp_subquery_expression->pqp, visited_ops, nodes_map, edges_list);
            { const auto &perf = *pqp_subquery_expression->pqp->performance_data; const auto rows = (pqp_subquery_expression->pqp->executed() && perf.has_output) ? perf.output_row_count : size_t{0}; edges_list.push_back({pqp_subquery_expression->pqp->operator_id, op->operator_id, rows}); }
            return ExpressionVisitation::VisitArguments; });
    }
    break;

    default:
    {
    } // OperatorType has no expressions
    }
}

} // namespace hyrise
