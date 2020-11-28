/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/SegmentsConnector.h"
#include "planner/planners/match/InnerJoinStrategy.h"
#include "planner/planners/match/AddDependencyStrategy.h"

namespace nebula {
namespace graph {
PlanNode* SegmentsConnector::innerJoinSegments(QueryContext* qctx,
                                            const PlanNode* left,
                                            const PlanNode* right) {
    return std::make_unique<InnerJoinStrategy>(qctx)->connect(left, right);
}

void SegmentsConnector::addDependency(const PlanNode* left, const PlanNode* right) {
    std::make_unique<AddDependencyStrategy>()->connect(left, right);
}
}  // namespace graph
}  // namespace nebula
