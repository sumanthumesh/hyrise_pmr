#pragma once

#include <string>
#include <vector>

namespace hyrise
{

class AbstractOperator;

struct ReadColumnsSnapshot
{
    std::vector<std::string> from_left;
    std::vector<std::string> from_right;
};

// Return the deduplicated source column names this operator reads from its LEFT input and
// RIGHT input respectively. Names are resolved through ReferenceSegments so they reflect the
// storage-table column names, not any intermediate rename. Must be called while the input
// operators are still ExecutedAndAvailable (typically from inside AbstractOperator::execute()
// after _on_execute() finishes) — after consumer deregistration, intermediate tables get
// cleared and this cannot be reconstructed. GetTable and pass-through/structural ops (Alias,
// UnionAll, UnionPositions, Validate, Limit) return empty vectors on purpose.
ReadColumnsSnapshot collect_read_columns(const AbstractOperator &op);

} // namespace hyrise
