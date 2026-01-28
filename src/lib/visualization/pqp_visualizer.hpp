#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace hyrise
{

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
                             std::vector<std::pair<size_t, size_t>> &edges_list);

    void _collect_subtree_info(const std::shared_ptr<const AbstractOperator> &op,
                               std::unordered_set<std::shared_ptr<const AbstractOperator>> &visited_ops,
                               std::unordered_map<size_t, std::pair<std::string, std::chrono::nanoseconds>> &nodes_map,
                               std::vector<std::pair<size_t, size_t>> &edges_list);

    std::unordered_map<std::string, std::chrono::nanoseconds> _duration_by_operator_name;
};

} // namespace hyrise
