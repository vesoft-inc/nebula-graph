/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIndexSeekPlanner.h"

#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {
bool MatchVertexIndexSeekPlanner::match(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    auto& head = matchCtx->nodeInfos[0];
    if (head.label == nullptr) {
        return false;
    }

    return true;
}

StatusOr<SubPlan> MatchVertexIndexSeekPlanner::transform(AstContext* astCtx) {
    // TODO:
    UNUSED(astCtx);
    return Status::Error();
}
}  // namespace graph
}  // namespace nebula
