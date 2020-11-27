/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/MatchClausePlanner.h"

#include "context/ast/QueryAstContext.h"
#include "planner/planners/match/Expand.h"
#include "planner/planners/match/StartVidFinder.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> MatchClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kMatch) {
        return Status::Error("Not a valid context for MatchClausePlanner.");
    }

    auto* matchClauseCtx = static_cast<MatchClauseContext*>(clauseCtx);
    SubPlan matchClausePlan;

    auto startIndex = -1;
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto& startVidFinders = StartVidFinder::finders();
    // Find the start plan node
    for (size_t i = 0; i < nodeInfos.size(); ++i) {
        for (auto& finder : startVidFinders) {
            auto nodeCtx = NodeContext(matchClauseCtx, &nodeInfos[i]);
            if (finder.match(&nodeCtx)) {
                auto plan = finder.instantiate()->transform(&nodeCtx);
                if (!plan.ok()) {
                    return plan.status();
                }
                matchClausePlan = std::move(plan).value();
                startIndex = i;
                break;
            }

            if (i != nodeInfos.size() - 1) {
                auto edgeCtx = EdgeContext(matchClauseCtx, &edgeInfos[i]);
                if (finder.match(&edgeCtx)) {
                    auto plan = finder.instantiate()->transform(&edgeCtx);
                    if (!plan.ok()) {
                        return plan.status();
                    }
                    matchClausePlan = std::move(plan).value();
                    startIndex = i;
                    break;
                }
            }
        }
        if (startIndex != -1) {
            break;
        }
    }
    if (startIndex < 0) {
        return Status::Error("Can't solve the start vids from the sentence: %s",
                             clauseCtx->sentence->toString().c_str());
    }

    NG_RETURN_IF_ERROR(expand(nodeInfos, edgeInfos, matchClauseCtx, startIndex, matchClausePlan));
    // TODO: append the last node plan.
    NG_RETURN_IF_ERROR(appendFilterPlan(matchClausePlan));

    return matchClausePlan;
}

Status MatchClausePlanner::expand(const std::vector<NodeInfo>& nodeInfos,
                                  const std::vector<EdgeInfo>& edgeInfos,
                                  MatchClauseContext* matchClauseCtx,
                                  int64_t startIndex,
                                  SubPlan& subplan) {
    // Do expand from startIndex and connect the the subplans.
    // TODO: Only support start from the head node now.
    if (startIndex != 0) {
        return Status::Error("Only support start from the head node parttern.");
    }

    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        auto status = std::make_unique<Expand>(matchClauseCtx, nullptr)->doExpand(
            nodeInfos[i], edgeInfos[i], subplan.root, &subplan);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status MatchClausePlanner::appendFilterPlan(SubPlan& plan) {
    UNUSED(plan);
    return Status::Error("TODO");
}
}  // namespace graph
}  // namespace nebula
