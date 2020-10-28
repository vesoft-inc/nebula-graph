/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIndexSeekPlanner.h"

namespace nebula {
namespace graph {
bool MatchVertexIndexSeekPlanner::match(AstContext* astCtx) {
    // TODO:
    UNUSED(astCtx);
    return false;
}

StatusOr<SubPlan> MatchVertexIndexSeekPlanner::transform(AstContext* astCtx) {
    // TODO:
    UNUSED(astCtx);
    return Status::Error();
}
}  // namespace graph
}  // namespace nebula
