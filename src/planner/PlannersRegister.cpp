/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlannersRegister.h"

#include "planner/Planner.h"
#include "planner/planners/SequentialPlanner.h"
#include "planner/planners/MatchVertexIdSeekPlannerPlanner.h"

namespace nebula {
namespace graph {
void PlannersRegister::registPlanners() {
    registSequential();
}

void PlannersRegister::registSequential() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kSequential];
    planners.emplace_back(&SequentialPlanner::match, &SequentialPlanner::make);
}

void PlannersRegister::registMatch() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kMatch];

    // MATCH(n) WHERE id(n) = value RETURN n
    planners.emplace_back(&MatchVertexIdSeekPlanner::match, &MatchVertexIdSeekPlanner::make);

    // MATCH(n:Tag{prop:value}) RETURN n
    // MATCH(n:Tag) WHERE n.prop = value RETURN n
    planners.emplace_back(&MatchVertexIndexSeekPlanner::match, &MatchVertexIndexSeekPlanner::make);
}
}  // namespace graph
}  // namespace nebula
