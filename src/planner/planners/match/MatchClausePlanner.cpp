/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/MatchClausePlanner.h"

#include "context/ast/QueryAstContext.h"
#include "planner/Query.h"
#include "planner/planners/match/Expand.h"
#include "planner/planners/match/MatchSolver.h"
#include "planner/planners/match/SegmentsConnector.h"
#include "planner/planners/match/StartVidFinder.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

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
                initialExpr_ = nodeCtx.initialExpr;
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

    std::vector<std::string> joinColNames = {folly::stringPrintf("%s_%d", kPathStr, 0)};
    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx, initialExpr_)
                          ->doExpand(nodeInfos[i], edgeInfos[i], subplan.root, &subplan);
        if (!status.ok()) {
            return status;
        }
        auto right = subplan.root;
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, i));
        subplan.root->setColNames(joinColNames);
    }

    auto left = subplan.root;
    NG_RETURN_IF_ERROR(appendFetchVertexPlan(
        nodeInfos.back().filter, matchClauseCtx->qctx, matchClauseCtx->space, &subplan));
    auto right = subplan.root;
    subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
    joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, edgeInfos.size()));
    subplan.root->setColNames(joinColNames);

    return Status::OK();
}

Status MatchClausePlanner::appendFetchVertexPlan(const Expression* nodeFilter,
                                                 QueryContext* qctx,
                                                 SpaceInfo& space,
                                                 SubPlan* plan) {
    MatchSolver::extractAndDedupVidColumn(qctx, &initialExpr_, plan);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    auto gv = GetVertices::make(qctx, plan->root, space.id, srcExpr.release(), {}, {});

    PlanNode* root = gv;
    if (nodeFilter != nullptr) {
        auto filter = nodeFilter->clone().release();
        RewriteMatchLabelVisitor visitor([](const Expression* expr) {
            DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
            auto la = static_cast<const LabelAttributeExpression*>(expr);
            auto attr = new LabelExpression(*la->right()->name());
            return new AttributeExpression(new VertexExpression(), attr);
        });
        filter->accept(&visitor);
        root = Filter::make(qctx, root, filter);
    }

    // normalize all columns to one
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto pathExpr = std::make_unique<PathBuildExpression>();
    pathExpr->add(std::make_unique<VertexExpression>());
    columns->addColumn(new YieldColumn(pathExpr.release()));
    plan->root = Project::make(qctx, root, columns);
    plan->root->setColNames({kPathStr});
    return Status::OK();
}

Status MatchClausePlanner::appendFilterPlan(SubPlan& plan) {
    UNUSED(plan);
    return Status::Error("TODO");
}
}  // namespace graph
}  // namespace nebula
