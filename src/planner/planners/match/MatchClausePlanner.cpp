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
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    SubPlan matchClausePlan;
    int64_t startIndex = -1;

    NG_RETURN_IF_ERROR(findStarts(matchClauseCtx, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(expand(nodeInfos, edgeInfos, matchClauseCtx, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, &matchClausePlan));
    NG_RETURN_IF_ERROR(MatchSolver::buildFilter(matchClauseCtx, &matchClausePlan));
    return matchClausePlan;
}

Status MatchClausePlanner::findStarts(MatchClauseContext* matchClauseCtx,
                                      int64_t& startIndex,
                                      SubPlan& matchClausePlan) {
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
                             matchClauseCtx->sentence->toString().c_str());
    }

    return Status::OK();
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

Status MatchClausePlanner::projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                                   SubPlan* plan) {
    auto qctx = matchClauseCtx->qctx;
    auto &nodeInfos = matchClauseCtx->nodeInfos;
    auto &edgeInfos = matchClauseCtx->edgeInfos;
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto input = plan->root;
    const auto &inColNames = input->colNamesRef();
    DCHECK_EQ(inColNames.size(), nodeInfos.size());
    std::vector<std::string> colNames;

    auto addNode = [&, this](size_t i) {
        auto &nodeInfo = nodeInfos[i];
        if (nodeInfo.alias != nullptr && !nodeInfo.anonymous) {
            columns->addColumn(buildVertexColumn(inColNames[i], *nodeInfo.alias));
            colNames.emplace_back(*nodeInfo.alias);
        }
    };

    for (size_t i = 0; i < edgeInfos.size(); i++) {
        addNode(i);
        auto &edgeInfo = edgeInfos[i];
        if (edgeInfo.alias != nullptr && !edgeInfo.anonymous) {
            columns->addColumn(buildEdgeColumn(inColNames[i], edgeInfo));
            colNames.emplace_back(*edgeInfo.alias);
        }
    }

    // last vertex
    DCHECK(!nodeInfos.empty());
    addNode(nodeInfos.size() - 1);

    const auto &aliases = matchClauseCtx->aliases;
    auto iter = std::find_if(aliases.begin(), aliases.end(), [](const auto &alias) {
        return alias.second == AliasType::kPath;
    });
    std::string alias = iter != aliases.end() ? iter->first : qctx->vctx()->anonColGen()->getCol();
    columns->addColumn(buildPathColumn(alias, input));
    colNames.emplace_back(alias);

    auto project = Project::make(qctx, input, columns);
    project->setColNames(std::move(colNames));

    plan->root = MatchSolver::filterCyclePath(project, alias, qctx);
    return Status::OK();
}

YieldColumn* MatchClausePlanner::buildVertexColumn(const std::string& colName,
                                                   const std::string& alias) const {
    auto colExpr = ExpressionUtils::inputPropExpr(colName);
    // startNode(path) => head node of path
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    return new YieldColumn(firstVertexExpr.release(), new std::string(alias));
}

YieldColumn* MatchClausePlanner::buildEdgeColumn(const std::string& colName, EdgeInfo& edge) const {
    auto colExpr = ExpressionUtils::inputPropExpr(colName);
    // relationships(p)
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("relationships");
    auto relExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    Expression* expr = nullptr;
    if (edge.range != nullptr) {
        expr = relExpr.release();
    } else {
        // Get first edge in path list [e1, e2, ...]
        auto idxExpr = std::make_unique<ConstantExpression>(0);
        auto subExpr = std::make_unique<SubscriptExpression>(relExpr.release(), idxExpr.release());
        expr = subExpr.release();
    }
    return new YieldColumn(expr, new std::string(*edge.alias));
}

YieldColumn* MatchClausePlanner::buildPathColumn(const std::string& alias,
                                                 const PlanNode* input) const {
    auto pathExpr = std::make_unique<PathBuildExpression>();
    for (const auto& colName : input->colNamesRef()) {
        pathExpr->add(ExpressionUtils::inputPropExpr(colName));
    }
    return new YieldColumn(pathExpr.release(), new std::string(alias));
}

Status MatchClausePlanner::appendFilterPlan(SubPlan& plan) {
    UNUSED(plan);
    return Status::Error("TODO");
}
}  // namespace graph
}  // namespace nebula
