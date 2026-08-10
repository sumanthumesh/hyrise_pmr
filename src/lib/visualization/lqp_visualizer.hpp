#pragma once

#include <iomanip>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace hyrise
{

class CardinalityEstimator;

struct LQPEdge
{
    size_t src_id;
    size_t dst_id;
    double estimated_row_count;  // cardinality estimate of rows flowing src -> dst; NaN if unavailable
};

class LQPVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractLQPNode>>>
{
  public:
    LQPVisualizer();

    LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                  VizEdgeInfo edge_info = {});

    void visualize(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots,
                   const std::string &img_filename) override;

    // JSON dump of the LQP graph:
    //   { "nodes": [{"id", "name"}, ...], "edges": [{"src", "dst"}, ...] }
    // Node ids are stable within one call — assigned in visit order (0..N-1).
    void export_as_graph_json(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots,
                              const std::string &json_filename);

  protected:
    void _build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>> &lqp_roots) override;

    void _build_subtree(const std::shared_ptr<AbstractLQPNode> &node,
                        std::unordered_set<std::shared_ptr<const AbstractLQPNode>> &visualized_nodes,
                        ExpressionUnorderedSet &visualized_sub_queries,
                        const CardinalityEstimator &cardinality_estimator);

    void _build_dataflow(const std::shared_ptr<AbstractLQPNode> &source_node,
                         const std::shared_ptr<AbstractLQPNode> &target_node, const InputSide side,
                         const CardinalityEstimator &cardinality_estimator);

    void _collect_subtree_info(
        const std::shared_ptr<const AbstractLQPNode> &node,
        const CardinalityEstimator &cardinality_estimator,
        std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> &node_id_map,
        std::vector<std::pair<std::shared_ptr<const AbstractLQPNode>, std::string>> &nodes_list,
        std::vector<LQPEdge> &edges_list);

    // Visit-order node ids assigned during _build_subtree so both the PNG label and the JSON
    // export agree on the same numbering.
    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> _node_ids;
};

} // namespace hyrise
