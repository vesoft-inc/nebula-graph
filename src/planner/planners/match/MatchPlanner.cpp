/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchPlanner.h"

namespace nebula {
namespace graph {
bool MatchPlanner::match(AstContext* astCtx) {
    UNUSED(astCtx);
    return true;
}

StatusOr<SubPlan> transform(AstContext* astCtx) {
    UNUSED(astCtx);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
