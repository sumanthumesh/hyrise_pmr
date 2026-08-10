#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace hyrise
{

struct PQPEdge
{
    size_t src_operator_id;
    size_t dst_operator_id;
    size_t row_count;   // rows flowing from src to dst; 0 if unexecuted
};

class PQPVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractOperator>>>
{
  public:
    PQPVisualizer();

    PQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                  VizEdgeInfo edge_info = {});

    /**
     * Exports the PQP graph as a text file with nodes and edges.
     * Format:
     *   NODES
     *   operator_id operator_type walltime_nanoseconds
     *   ...
     *   EDGES
     *   src_operator_id dest_operator_id
     *   ...
     */
    void export_as_graph_text(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                              const std::string &text_filename);

    // JSON dump of the same graph:
    //   { "nodes": [{"id", "name", "walltime_ns"}, ...],
    //     "edges": [{"src", "dst"}, ...] }
    void export_as_graph_json(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                              const std::string &json_filename);

    // Nested-tree JSON dump. Each node emits:
    //   { "description", "Ops"?, "Leftchildren"?, "LeftCard"?, "Rightchildren"?, "RightCard"? }
    // Shared subtrees (diamond PQPs) are emitted only once — the second visit yields {}.
    // Subquery edges are NOT included in the tree (they don't have a fixed left/right slot).
    void export_as_hierarchical_json(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                                     const std::string &json_filename);

    void visualize(const std::vector<std::shared_ptr<AbstractOperator>> &plans, const std::string &img_filename) override;

  protected:
    void _build_graph(const std::vector<std::shared_ptr<AbstractOperator>> &plans) override;

    void _build_subtree(const std::shared_ptr<const AbstractOperator> &op,
                        std::unordered_set<std::shared_ptr<const AbstractOperator>> &visualized_ops);

    void _visualize_subqueries(const std::shared_ptr<const AbstractOperator> &op,
                               const std::shared_ptr<AbstractExpression> &expression,
                               std::unordered_set<std::shared_ptr<const AbstractOperator>> &visualized_ops);

    void _build_dataflow(const std::shared_ptr<const AbstractOperator> &source_node,
                         const std::shared_ptr<const AbstractOperator> &target_node, const InputSide side);

    void _add_operator(const std::shared_ptr<const AbstractOperator> &op);

    // Helper method to collect graph information for export
    void _collect_graph_info(const std::vector<std::shared_ptr<AbstractOperator>> &plans,
                             std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> &nodes_map,
                             std::vector<PQPEdge> &edges_list);

    void _collect_subtree_info(const std::shared_ptr<const AbstractOperator> &op,
                               std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops,
                               std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> &nodes_map,
                               std::vector<PQPEdge> &edges_list);

    nlohmann::json _build_hierarchical_subtree(
        const std::shared_ptr<const AbstractOperator> &op,
        std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops);

    std::unordered_map<std::string, std::chrono::nanoseconds> _duration_by_operator_name;
};

} // namespace hyrise
