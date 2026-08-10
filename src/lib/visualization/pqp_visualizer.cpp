#include "visualization/pqp_visualizer.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <locale>
#include <memory>
#include <ratio>
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
#include "expression/expression_utils.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
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
    auto txt_filename = img_filename;
    const auto last_dot = txt_filename.find_last_of('.');
    if (last_dot != std::string::npos)
    {
        txt_filename = txt_filename.substr(0, last_dot);
    }
    txt_filename += ".graph";
    export_as_graph_text(plans, txt_filename);

    auto json_filename = img_filename;
    const auto last_dot_json = json_filename.find_last_of('.');
    if (last_dot_json != std::string::npos)
    {
        json_filename = json_filename.substr(0, last_dot_json);
    }
    json_filename += ".graph.json";
    export_as_graph_json(plans, json_filename);

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

nlohmann::json PQPVisualizer::_build_hierarchical_subtree(
    const std::shared_ptr<const AbstractOperator> &op,
    std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops)
{
    // Diamond-shaped PQPs: shared subtrees are emitted only once.
    if (visited_ops.contains(op))
    {
        return nlohmann::json::object();
    }
    visited_ops.insert(op);

    auto node = nlohmann::json::object();
    node["id"] = op->operator_id;
    node["name"] = std::string{op->name()};
    node["walltime_ns"] = op->performance_data->walltime.count();
    node["description"] = op->description();

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

    switch (op->type())
    {
    case OperatorType::Projection:
        node["Ops"] = "projection";
        break;
    case OperatorType::TableScan:
        node["Ops"] = "TableScan";
        break;
    case OperatorType::Limit:
        node["Ops"] = "Limit";
        break;
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
