#include "lqp_visualizer.hpp"

#include <cmath>
#include <fstream>
#include <iomanip>
#include <ios>
#include <locale>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "magic_enum/magic_enum.hpp"
#include "nlohmann/json.hpp"

#include <set>

#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "types.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace hyrise
{

LQPVisualizer::LQPVisualizer()
{
    // Set defaults for this visualizer
    _default_vertex.shape = "rectangle";
}

LQPVisualizer::LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void LQPVisualizer::visualize(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots,
                              const std::string &img_filename)
{
    AbstractVisualizer::visualize(lqp_roots, img_filename);

    // auto json_filename = img_filename;
    // const auto last_dot = json_filename.find_last_of('.');
    // if (last_dot != std::string::npos)
    // {
    //     json_filename = json_filename.substr(0, last_dot);
    // }
    // json_filename += ".graph.json";
    // export_as_graph_json(lqp_roots, json_filename);

    auto tree_filename = img_filename;
    const auto last_dot_tree = tree_filename.find_last_of('.');
    if (last_dot_tree != std::string::npos)
    {
        tree_filename = tree_filename.substr(0, last_dot_tree);
    }
    tree_filename += ".tree.json";
    export_as_hierarchical_json(lqp_roots, tree_filename);
}

void LQPVisualizer::export_as_graph_json(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots,
                                         const std::string &json_filename)
{
    // PNG labels and JSON ids stay in sync because _build_subtree (PNG pass, ran first inside
    // visualize()) and _collect_subtree_info (below) walk the tree in the same order
    // (dedupe -> left -> right -> subquery), producing identical id sequences.
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> node_id_map;
    std::vector<std::pair<std::shared_ptr<const AbstractLQPNode>, std::string>> nodes_list;
    std::vector<LQPEdge> edges_list;

    for (const auto &root : lqp_roots)
    {
        const auto cardinality_estimator = CardinalityEstimator{};
        cardinality_estimator.guarantee_bottom_up_construction(root);
        _collect_subtree_info(root, cardinality_estimator, node_id_map, nodes_list, edges_list);
    }

    auto nodes_json = nlohmann::json::array();
    for (const auto &[node, description] : nodes_list)
    {
        nodes_json.push_back({
            {"id", node_id_map[node]},
            {"name", description},
        });
    }

    auto edges_json = nlohmann::json::array();
    for (const auto &edge : edges_list)
    {
        // Emit NaN as null so the JSON stays valid.
        auto rows_field = std::isnan(edge.estimated_row_count)
                              ? nlohmann::json{nullptr}
                              : nlohmann::json{edge.estimated_row_count};
        edges_json.push_back({
            {"src", edge.src_id},
            {"dst", edge.dst_id},
            {"estimated_rows", rows_field},
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

// Format the estimated cardinality of an edge (source -> parent) as a short string,
// matching the graphviz label style. Returns "" when the estimate is NaN.
static std::string card_label(double estimated_rows)
{
    if (std::isnan(estimated_rows))
    {
        return {};
    }
    auto stream = std::ostringstream{};
    stream << std::fixed << std::setprecision(1) << estimated_rows << " row(s) est.";
    return stream.str();
}

LQPVisualizer::LQPColumnsSplit LQPVisualizer::_collect_columns(
    const std::shared_ptr<const AbstractLQPNode> &node)
{
    // LQPColumnExpression::description() resolves through the origin StoredTableNode to the
    // storage-column name. For each column expression we route to `left_names` or `right_names`
    // depending on which input can find that expression in its output. For unary nodes (no
    // right input) everything lands on left; for a JoinNode's predicate the two sides split
    // naturally by find_column_id lookup.
    std::set<std::string> left_names;
    std::set<std::string> right_names;

    const auto left_input = node->left_input();
    const auto right_input = node->right_input();

    for (const auto &expression : node->node_expressions)
    {
        auto mut = expression;
        visit_expression(mut, [&](const auto &sub) {
            const auto lqp_col = std::dynamic_pointer_cast<LQPColumnExpression>(sub);
            if (!lqp_col)
            {
                return ExpressionVisitation::VisitArguments;
            }
            const auto name = lqp_col->description(AbstractExpression::DescriptionMode::ColumnName);

            const auto in_left = left_input && left_input->find_column_id(*lqp_col).has_value();
            const auto in_right = right_input && right_input->find_column_id(*lqp_col).has_value();

            if (in_left)
            {
                left_names.insert(name);
            }
            if (in_right)
            {
                right_names.insert(name);
            }
            // If it matched neither (e.g., the column belongs to an ancestor scope for a
            // correlated subquery), default to left so it's not silently dropped.
            if (!in_left && !in_right)
            {
                left_names.insert(name);
            }
            return ExpressionVisitation::VisitArguments;
        });
    }

    return {std::vector<std::string>{left_names.begin(), left_names.end()},
            std::vector<std::string>{right_names.begin(), right_names.end()}};
}

nlohmann::json LQPVisualizer::_build_hierarchical_subtree(
    const std::shared_ptr<const AbstractLQPNode> &node,
    const CardinalityEstimator &cardinality_estimator,
    std::unordered_set<std::shared_ptr<const AbstractLQPNode>> &visited_nodes)
{
    // Diamond-shaped LQPs: shared subtrees are emitted only once.
    if (visited_nodes.contains(node))
    {
        return nlohmann::json::object();
    }
    visited_nodes.insert(node);

    // Use the same visit-order id that _build_subtree (PNG pass) assigned, so PNG labels and
    // JSON ids agree.
    const auto id_it = _node_ids.find(node);
    const auto assigned_id =
        (id_it != _node_ids.end()) ? id_it->second : _node_ids.size();
    if (id_it == _node_ids.end())
    {
        _node_ids[node] = assigned_id;
    }

    auto out = nlohmann::json::object();
    out["id"] = assigned_id;
    out["name"] = std::string{magic_enum::enum_name(node->type)};
    out["description"] = node->description();
    const auto columns_split = _collect_columns(node);
    out["columns_left"] = columns_split.from_left;
    out["columns_right"] = columns_split.from_right;
    // Kept for backward compat: merged (dedup) list of both sides.
    std::set<std::string> merged{columns_split.from_left.begin(), columns_split.from_left.end()};
    merged.insert(columns_split.from_right.begin(), columns_split.from_right.end());
    out["columns"] = std::vector<std::string>{merged.begin(), merged.end()};

    if (node->left_input())
    {
        const auto left = node->left_input();
        out["Leftchildren"] = _build_hierarchical_subtree(left, cardinality_estimator, visited_nodes);
        out["LeftCard"] = card_label(cardinality_estimator.estimate_cardinality(left));
    }

    if (node->right_input())
    {
        const auto right = node->right_input();
        out["Rightchildren"] = _build_hierarchical_subtree(right, cardinality_estimator, visited_nodes);
        out["RightCard"] = card_label(cardinality_estimator.estimate_cardinality(right));
    }

    return out;
}

void LQPVisualizer::export_as_hierarchical_json(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots,
                                                const std::string &json_filename)
{
    std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visited_nodes;
    auto trees_json = nlohmann::json::array();
    for (const auto &root : lqp_roots)
    {
        const auto cardinality_estimator = CardinalityEstimator{};
        cardinality_estimator.guarantee_bottom_up_construction(root);
        trees_json.push_back(_build_hierarchical_subtree(root, cardinality_estimator, visited_nodes));
    }

    auto out_json = (lqp_roots.size() == 1) ? trees_json.front() : nlohmann::json{trees_json};

    auto file = std::ofstream(json_filename);
    Assert(file.is_open(), "Failed to open file for writing: " + json_filename);
    file << out_json.dump(2) << "\n";
}

void LQPVisualizer::_collect_subtree_info(
    const std::shared_ptr<const AbstractLQPNode> &node,
    const CardinalityEstimator &cardinality_estimator,
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> &node_id_map,
    std::vector<std::pair<std::shared_ptr<const AbstractLQPNode>, std::string>> &nodes_list,
    std::vector<LQPEdge> &edges_list)
{
    // Avoid processing nodes redundantly in diamond-shaped plans.
    if (node_id_map.contains(node))
    {
        return;
    }
    const auto this_id = nodes_list.size();
    node_id_map[node] = this_id;
    nodes_list.emplace_back(node, node->description());

    if (node->left_input())
    {
        const auto left = node->left_input();
        _collect_subtree_info(left, cardinality_estimator, node_id_map, nodes_list, edges_list);
        edges_list.push_back({node_id_map[left], this_id, cardinality_estimator.estimate_cardinality(left)});
    }

    if (node->right_input())
    {
        const auto right = node->right_input();
        _collect_subtree_info(right, cardinality_estimator, node_id_map, nodes_list, edges_list);
        edges_list.push_back({node_id_map[right], this_id, cardinality_estimator.estimate_cardinality(right)});
    }

    // Subquery edges (dashed in the graphviz version).
    for (const auto &expression : node->node_expressions)
    {
        visit_expression(expression, [&](const auto &sub_expression) {
            const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
            if (!subquery_expression)
            {
                return ExpressionVisitation::VisitArguments;
            }
            _collect_subtree_info(subquery_expression->lqp, cardinality_estimator, node_id_map, nodes_list, edges_list);
            edges_list.push_back({node_id_map[subquery_expression->lqp], this_id,
                                  cardinality_estimator.estimate_cardinality(subquery_expression->lqp)});
            return ExpressionVisitation::VisitArguments;
        });
    }
}

void LQPVisualizer::_build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots)
{
    _node_ids.clear();

    std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visualized_nodes;
    ExpressionUnorderedSet visualized_sub_queries;

    for (const auto &root : lqp_roots)
    {
        const auto cardinality_estimator = CardinalityEstimator{};
        cardinality_estimator.guarantee_bottom_up_construction(root);
        _build_subtree(root, visualized_nodes, visualized_sub_queries, cardinality_estimator);
    }
}

void LQPVisualizer::_build_subtree(const std::shared_ptr<AbstractLQPNode> &node,
                                   std::unordered_set<std::shared_ptr<const AbstractLQPNode>> &visualized_nodes,
                                   ExpressionUnorderedSet &visualized_sub_queries,
                                   const CardinalityEstimator &cardinality_estimator)
{
    // Avoid drawing dataflows/nodes redundantly in diamond-shaped plans.
    if (visualized_nodes.contains(node))
    {
        return;
    }
    visualized_nodes.insert(node);

    const auto node_id = _node_ids.size();
    _node_ids[node] = node_id;

    auto node_label = std::to_string(node_id) + "\n" + node->description();
    if (!node->comment.empty())
    {
        node_label += "\n(" + node->comment + ")";
    }
    _add_vertex(node, node_label);

    if (node->left_input())
    {
        auto left_input = node->left_input();
        _build_subtree(left_input, visualized_nodes, visualized_sub_queries, cardinality_estimator);
        _build_dataflow(left_input, node, InputSide::Left, cardinality_estimator);
    }

    if (node->right_input())
    {
        auto right_input = node->right_input();
        _build_subtree(right_input, visualized_nodes, visualized_sub_queries, cardinality_estimator);
        _build_dataflow(right_input, node, InputSide::Right, cardinality_estimator);
    }

    // Visualize subqueries.
    for (const auto &expression : node->node_expressions)
    {
        visit_expression(expression, [&](const auto &sub_expression)
                         {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) {
        return ExpressionVisitation::VisitArguments;
      }

      if (!visualized_sub_queries.emplace(subquery_expression).second) {
        return ExpressionVisitation::VisitArguments;
      }

      _build_subtree(subquery_expression->lqp, visualized_nodes, visualized_sub_queries, cardinality_estimator);

      auto edge_info = _default_edge;
      auto correlated_str = std::string(subquery_expression->is_correlated() ? "correlated" : "uncorrelated");
      edge_info.label = correlated_str + " subquery";
      edge_info.style = "dashed";
      _add_edge(subquery_expression->lqp, node, edge_info);

      return ExpressionVisitation::VisitArguments; });
    }
}

void LQPVisualizer::_build_dataflow(const std::shared_ptr<AbstractLQPNode> &source_node,
                                    const std::shared_ptr<AbstractLQPNode> &target_node, const InputSide side,
                                    const CardinalityEstimator &cardinality_estimator)
{
    Cardinality row_count = NAN;
    auto row_percentage = 100.0;

    row_count = cardinality_estimator.estimate_cardinality(source_node);
    if (source_node->left_input())
    {
        auto input_count = cardinality_estimator.estimate_cardinality(source_node->left_input());

        const auto join_node = std::dynamic_pointer_cast<JoinNode>(source_node);
        // Include right side in cardinality estimation for unions and joins (unless it is a semi-/anti-join).
        if (source_node->right_input() && source_node->type == LQPNodeType::Union)
        {
            input_count += cardinality_estimator.estimate_cardinality(source_node->right_input());
        }
        else if (source_node->right_input() && (!join_node || (join_node->join_mode != JoinMode::Semi &&
                                                               join_node->join_mode != JoinMode::AntiNullAsTrue &&
                                                               join_node->join_mode != JoinMode::AntiNullAsFalse)))
        {
            input_count *= cardinality_estimator.estimate_cardinality(source_node->right_input());
        }
        row_percentage = input_count == 0 ? 0 : 100 * row_count / input_count;
    }

    auto label_stream = std::ostringstream{};

    // Use a copy of the stream's default locale with thousands separators: Dynamically allocated raw pointers should
    // be avoided whenever possible. Unfortunately, std::locale stores pointers to the facets and does internal
    // reference counting. std::locale's destructor destructs the locale and the facets whose reference count becomes
    // zero. This forces us to use a dynamically allocated raw pointer here.
    const auto &separate_thousands_locale = std::locale(label_stream.getloc(), new SeparateThousandsFacet);
    label_stream.imbue(separate_thousands_locale);

    if (!std::isnan(row_count))
    {
        label_stream << " " << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage
                     << "% estd.";
    }
    else
    {
        label_stream << "no est.";
    }

    auto tooltip_stream = std::stringstream{};

    // Edge Tooltip: Node Output Expressions
    tooltip_stream << "Output Expressions: \n";
    const auto &output_expressions = source_node->output_expressions();
    const auto output_expression_count = output_expressions.size();
    for (auto column_id = ColumnID{0}; column_id < output_expression_count; ++column_id)
    {
        tooltip_stream << " (" << column_id + 1 << ") ";
        tooltip_stream << output_expressions.at(column_id)->as_column_name();
        if (source_node->is_column_nullable(column_id))
        {
            tooltip_stream << " NULL";
        }
        tooltip_stream << "\n";
    }

    if (!dynamic_pointer_cast<AbstractNonQueryNode>(source_node))
    {
        // Edge Tooltip: Unique Column Combinations.
        const auto &unique_column_combinations = source_node->unique_column_combinations();
        tooltip_stream << "\n"
                       << "Unique Column Combinations: \n";
        if (unique_column_combinations.empty())
        {
            tooltip_stream << " <none>\n";
        }

        auto ucc_idx = 1;
        for (const auto &ucc : unique_column_combinations)
        {
            tooltip_stream << " (" << ucc_idx << ") ";
            tooltip_stream << ucc << "\n";
            ++ucc_idx;
        }

        // Edge Tooltip: Trivial FDs.
        auto trivial_fds = FunctionalDependencies();
        if (!unique_column_combinations.empty())
        {
            trivial_fds = fds_from_unique_column_combinations(source_node, unique_column_combinations);
        }
        tooltip_stream << "\n"
                       << "Functional Dependencies (trivial): \n";
        if (trivial_fds.empty())
        {
            tooltip_stream << " <none>\n";
        }

        auto trivial_fd_idx = 1;
        for (const auto &fd : trivial_fds)
        {
            tooltip_stream << " (" << trivial_fd_idx << ") ";
            tooltip_stream << fd << "\n";
            ++trivial_fd_idx;
        }

        // Edge Tooltip: Non-trivial FDs
        const auto &fds = source_node->non_trivial_functional_dependencies();
        tooltip_stream << "\n"
                       << "Functional Dependencies (non-trivial): \n";
        if (fds.empty())
        {
            tooltip_stream << " <none>";
        }

        auto fd_idx = 1;
        for (const auto &fd : fds)
        {
            tooltip_stream << " (" << fd_idx << ") ";
            tooltip_stream << fd << "\n";
            ++fd_idx;
        }
    }

    auto info = _default_edge;
    info.label = label_stream.str();
    info.label_tooltip = tooltip_stream.str();
    // `pen_width` is normalized later during graph construction, so assigning 0 or NAN is acceptable.
    info.pen_width = row_count;
    if (target_node->input_count() == 2)
    {
        info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
    }

    _add_edge(source_node, target_node, info);
}

} // namespace hyrise
