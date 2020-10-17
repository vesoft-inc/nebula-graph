/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/GoOneStepPlanner.h"

#include "parser/Sentence.h"

namespace nebula {
namespace graph {
std::unique_ptr<GoOneStepPlanner> GoOneStepPlanner::instance_ =
    std::unique_ptr<GoOneStepPlanner>(new GoOneStepPlanner());

GoOneStepPlanner::GoOneStepPlanner() {
    plannersMap()[Sentence::Kind::kGo].emplace_back(this);
}

bool GoOneStepPlanner::match(Validator* validator) {
    UNUSED(validator);
    // TODO:
    return false;
}

SubPlan GoOneStepPlanner::transform() {
    // TODO:
    SubPlan subPlan;
    return subPlan;
}
}  // namespace graph
}  // namespace nebula
