/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlannersRegister.h"

#include "planner/Planner.h"
#include "planner/planners/SequentialPlanner.h"
#include "planner/planners/match/MatchPlanner.h"
#include "planner/planners/match/StartVidFinder.h"
#include "planner/planners/match/ValueIndexSeek.h"
#include "planner/planners/match/VertexIdSeek.h"

namespace nebula {
namespace graph {
void PlannersRegister::registPlanners() {
    registSequential();
    registMatch();
}

void PlannersRegister::registSequential() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kSequential];
    planners.emplace_back(&SequentialPlanner::match, &SequentialPlanner::make);
}

void PlannersRegister::registMatch() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kMatch];

    planners.emplace_back(&MatchPlanner::match, &MatchPlanner::make);

    auto& startVidFinders = StartVidFinder::finders();

    // MATCH(n) WHERE id(n) = value RETURN n
    startVidFinders.emplace_back(&VertexIdSeek::match, &VertexIdSeek::make);

    // MATCH(n:Tag{prop:value}) RETURN n
    // MATCH(n:Tag) WHERE n.prop = value RETURN n
    startVidFinders.emplace_back(&ValueIndexSeek::match, &ValueIndexSeek::make);

    // MATCH(n:Tag) RETURN n;
    // planners.emplace_back(&MatchTagScanPlanner::match, &MatchTagScanPlanner::make);
}
}  // namespace graph
}  // namespace nebula
