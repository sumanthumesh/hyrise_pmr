#include "expression_evaluator_table_scan_impl.hpp"

#include <memory>
#include <string>

#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "memory/mem_manager.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise
{

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(
    const std::shared_ptr<const Table> &in_table, const std::shared_ptr<const AbstractExpression> &expression)
    : _in_table(in_table), _expression(expression) {}

std::string ExpressionEvaluatorTableScanImpl::description() const
{
    return "ExpressionEvaluator";
}

std::shared_ptr<RowIDPosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id)
{
    // Pass runtime_mr into the evaluator so its returned RowIDPosList is built directly
    // on the runtime exec resource — no copy needed. The outer make_on then wraps the
    // shared_ptr control block on the same pool.
    auto *runtime_mr = MemManager::get().pick_runtime_exec_resource();
    return RowIDPosList::make_on(
        runtime_mr, ExpressionEvaluator{_in_table, chunk_id, runtime_mr}.evaluate_expression_to_pos_list(*_expression));
}

} // namespace hyrise
