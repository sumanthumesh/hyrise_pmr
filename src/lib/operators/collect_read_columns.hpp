#pragma once

#include <string>
#include <vector>

namespace hyrise
{

class AbstractOperator;

// Return the deduplicated source column names this operator reads from its input tables.
// Names are resolved through ReferenceSegments so they reflect the storage-table column names,
// not any intermediate rename. Must be called while the input operators are still
// ExecutedAndAvailable (typically from inside AbstractOperator::execute() after _on_execute()
// finishes) — after consumer deregistration, intermediate tables get cleared and this cannot
// be reconstructed. GetTable and pass-through/structural ops (Alias, UnionAll, UnionPositions,
// Validate, Limit) return an empty vector on purpose.
std::vector<std::string> collect_read_columns(const AbstractOperator &op);

} // namespace hyrise
