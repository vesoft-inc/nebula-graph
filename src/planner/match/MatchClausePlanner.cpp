/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchClausePlanner.h"

#include "context/ast/QueryAstContext.h"
#include "planner/Query.h"
#include "planner/match/Expand.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/SegmentsConnector.h"
#include "planner/match/StartVidFinder.h"
#include "planner/match/WhereClausePlanner.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteVisitor.h"

using JoinStrategyPos = nebula::graph::InnerJoinStrategy::JoinPos;

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
    size_t startIndex = 0;
    bool startFromEdge = false;

    NG_RETURN_IF_ERROR(findStarts(matchClauseCtx, startFromEdge, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(
        expand(nodeInfos, edgeInfos, matchClauseCtx, startFromEdge, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(appendFilterPlan(matchClauseCtx, matchClausePlan));
    return matchClausePlan;
}

Status MatchClausePlanner::findStarts(MatchClauseContext* matchClauseCtx,
                                      bool& startFromEdge,
                                      size_t& startIndex,
                                      SubPlan& matchClausePlan) {
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto& startVidFinders = StartVidFinder::finders();
    bool foundStart = false;
    // Find the start plan node
    for (auto& finder : startVidFinders) {
        for (size_t i = 0; i < nodeInfos.size() && !foundStart; ++i) {
            auto nodeCtx = NodeContext(matchClauseCtx, &nodeInfos[i]);
            auto nodeFinder = finder();
            if (nodeFinder->match(&nodeCtx)) {
                auto plan = nodeFinder->transform(&nodeCtx);
                if (!plan.ok()) {
                    return plan.status();
                }
                matchClausePlan = std::move(plan).value();
                startIndex = i;
                foundStart = true;
                initialExpr_ = nodeCtx.initialExpr->clone();
                VLOG(1) << "Find starts: " << startIndex << ", Pattern has " << edgeInfos.size()
                        << " edges, root: " << matchClausePlan.root->outputVar()
                        << ", colNames: " << folly::join(",", matchClausePlan.root->colNames());
                break;
            }

            if (i != nodeInfos.size() - 1) {
                auto edgeCtx = EdgeContext(matchClauseCtx, &edgeInfos[i]);
                auto edgeFinder = finder();
                if (edgeFinder->match(&edgeCtx)) {
                    auto plan = edgeFinder->transform(&edgeCtx);
                    if (!plan.ok()) {
                        return plan.status();
                    }
                    matchClausePlan = std::move(plan).value();
                    startFromEdge = true;
                    startIndex = i;
                    foundStart = true;
                    break;
                }
            }
        }
        if (foundStart) {
            break;
        }
    }
    if (!foundStart) {
        return Status::SemanticError("Can't solve the start vids from the sentence: %s",
                                     matchClauseCtx->sentence->toString().c_str());
    }

    return Status::OK();
}

Status MatchClausePlanner::expand(const std::vector<NodeInfo>& nodeInfos,
                                  const std::vector<EdgeInfo>& edgeInfos,
                                  MatchClauseContext* matchClauseCtx,
                                  bool startFromEdge,
                                  size_t startIndex,
                                  SubPlan& subplan) {
    if (startFromEdge) {
        return expandFromEdge(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
    } else {
        return expandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
    }
}

Status MatchClausePlanner::expandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                          const std::vector<EdgeInfo>& edgeInfos,
                                          MatchClauseContext* matchClauseCtx,
                                          size_t startIndex,
                                          SubPlan& subplan) {
    DCHECK(!nodeInfos.empty() && startIndex < nodeInfos.size());
    if (startIndex == 0) {
        // Pattern: (start)-[]-...-()
        return rightExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
    }

    const auto& var = subplan.root->outputVar();
    if (startIndex == nodeInfos.size() - 1) {
        // Pattern: ()-[]-...-(start)
        return leftExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, var, subplan);
    }

    // Pattern: ()-[]-...-(start)-...-[]-()
    NG_RETURN_IF_ERROR(
        rightExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan));
    auto left = subplan.root;
    NG_RETURN_IF_ERROR(
        leftExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, var, subplan));

    // Connect the left expand and right expand part.
    auto right = subplan.root;
    subplan.root = SegmentsConnector::innerJoinSegments(
        matchClauseCtx->qctx, left, right, JoinStrategyPos::kStart, JoinStrategyPos::kStart);
    return Status::OK();
}

Status MatchClausePlanner::leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                              const std::vector<EdgeInfo>& edgeInfos,
                                              MatchClauseContext* matchClauseCtx,
                                              size_t startIndex,
                                              std::string inputVar,
                                              SubPlan& subplan) {
    std::vector<std::string> joinColNames = {
        folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size())};
    for (size_t i = startIndex; i > 0; --i) {
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx,
                                               i == startIndex ? initialExpr_->clone() : nullptr)
                          ->depends(subplan.root)
                          ->inputVar(inputVar)
                          ->reversely()
                          ->doExpand(nodeInfos[i], edgeInfos[i - 1], &subplan);
        if (!status.ok()) {
            return status;
        }
        if (i < startIndex) {
            auto right = subplan.root;
            VLOG(1) << "left: " << folly::join(",", left->colNames())
                    << " right: " << folly::join(",", right->colNames());
            subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
            joinColNames.emplace_back(
                folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size() + i));
            subplan.root->setColNames(joinColNames);
        }
        inputVar = subplan.root->outputVar();
    }

    VLOG(1) << subplan;
    auto left = subplan.root;
    NG_RETURN_IF_ERROR(MatchSolver::appendFetchVertexPlan(
        nodeInfos.front().filter,
        matchClauseCtx->space,
        matchClauseCtx->qctx,
        edgeInfos.empty() ? initialExpr_->clone().release() : nullptr,
        subplan));
    if (!edgeInfos.empty()) {
        auto right = subplan.root;
        VLOG(1) << "left: " << folly::join(",", left->colNames())
                << " right: " << folly::join(",", right->colNames());
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(
            folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size() + startIndex));
        subplan.root->setColNames(joinColNames);
    }

    VLOG(1) << subplan;
    return Status::OK();
}

Status MatchClausePlanner::rightExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                               const std::vector<EdgeInfo>& edgeInfos,
                                               MatchClauseContext* matchClauseCtx,
                                               size_t startIndex,
                                               SubPlan& subplan) {
    std::vector<std::string> joinColNames = {folly::stringPrintf("%s_%lu", kPathStr, startIndex)};
    for (size_t i = startIndex; i < edgeInfos.size(); ++i) {
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx,
                                               i == startIndex ? initialExpr_->clone() : nullptr)
                          ->depends(subplan.root)
                          ->inputVar(subplan.root->outputVar())
                          ->doExpand(nodeInfos[i], edgeInfos[i], &subplan);
        if (!status.ok()) {
            return status;
        }
        if (i > startIndex) {
            auto right = subplan.root;
            VLOG(1) << "left: " << folly::join(",", left->colNames())
                    << " right: " << folly::join(",", right->colNames());
            subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
            joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, i));
            subplan.root->setColNames(joinColNames);
        }
    }

    VLOG(1) << subplan;
    auto left = subplan.root;
    NG_RETURN_IF_ERROR(MatchSolver::appendFetchVertexPlan(
        nodeInfos.back().filter,
        matchClauseCtx->space,
        matchClauseCtx->qctx,
        edgeInfos.empty() ? initialExpr_->clone().release() : nullptr,
        subplan));
    if (!edgeInfos.empty()) {
        auto right = subplan.root;
        VLOG(1) << "left: " << folly::join(",", left->colNames())
                << " right: " << folly::join(",", right->colNames());
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, edgeInfos.size()));
        subplan.root->setColNames(joinColNames);
    }

    VLOG(1) << subplan;
    return Status::OK();
}

Status MatchClausePlanner::expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                                          const std::vector<EdgeInfo>& edgeInfos,
                                          MatchClauseContext* matchClauseCtx,
                                          size_t startIndex,
                                          SubPlan& subplan) {
    UNUSED(nodeInfos);
    UNUSED(edgeInfos);
    UNUSED(matchClauseCtx);
    UNUSED(startIndex);
    UNUSED(subplan);
    return Status::Error("Expand from edge has not been implemented yet.");
}

Status MatchClausePlanner::projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                                   size_t startIndex,
                                                   SubPlan& plan) {
    auto qctx = matchClauseCtx->qctx;
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto input = plan.root;
    const auto& inColNames = input->colNamesRef();
    auto columns = qctx->objPool()->add(new YieldColumns);
    std::vector<std::string> colNames;

    auto addNode = [&, this](size_t i) {
        auto& nodeInfo = nodeInfos[i];
        if (nodeInfo.alias != nullptr && !nodeInfo.anonymous) {
            if (i >= startIndex) {
                columns->addColumn(buildVertexColumn(inColNames[i - startIndex], *nodeInfo.alias));
            } else if (startIndex == (nodeInfos.size() - 1)) {
                columns->addColumn(buildVertexColumn(inColNames[startIndex - i], *nodeInfo.alias));
            } else {
                columns->addColumn(
                    buildVertexColumn(inColNames[nodeInfos.size() - i], *nodeInfo.alias));
            }
            colNames.emplace_back(*nodeInfo.alias);
        }
    };

    for (size_t i = 0; i < edgeInfos.size(); i++) {
        VLOG(1) << "colSize: " << inColNames.size() << "i: " << i
                << " nodesize: " << nodeInfos.size() << " start: " << startIndex;
        addNode(i);
        auto& edgeInfo = edgeInfos[i];
        if (edgeInfo.alias != nullptr && !edgeInfo.anonymous) {
            if (i >= startIndex) {
                columns->addColumn(buildEdgeColumn(inColNames[i - startIndex], edgeInfo));
            } else if (startIndex == (nodeInfos.size() - 1)) {
                columns->addColumn(buildEdgeColumn(inColNames[edgeInfos.size() - 1 - i], edgeInfo));
            } else {
                columns->addColumn(buildEdgeColumn(inColNames[edgeInfos.size() - i], edgeInfo));
            }
            colNames.emplace_back(*edgeInfo.alias);
        }
    }

    // last vertex
    DCHECK(!nodeInfos.empty());
    addNode(nodeInfos.size() - 1);

    const auto& aliases = matchClauseCtx->aliasesGenerated;
    auto iter = std::find_if(aliases.begin(), aliases.end(), [](const auto& alias) {
        return alias.second == AliasType::kPath;
    });
    std::string alias = iter != aliases.end() ? iter->first : qctx->vctx()->anonColGen()->getCol();
    columns->addColumn(buildPathColumn(alias, startIndex, inColNames, nodeInfos.size()));
    colNames.emplace_back(alias);

    auto project = Project::make(qctx, input, columns);
    project->setColNames(std::move(colNames));

    plan.root = MatchSolver::filtPathHasSameEdge(project, alias, qctx);
    VLOG(1) << plan;
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
                                                 size_t startIndex,
                                                 const std::vector<std::string> colNames,
                                                 size_t nodeInfoSize) const {
    auto colSize = colNames.size();
    DCHECK((nodeInfoSize == colSize) || (nodeInfoSize + 1 == colSize));
    size_t bound = 0;
    if (colSize > nodeInfoSize) {
        bound = colSize - startIndex - 1;
    } else if (startIndex == (nodeInfoSize - 1)) {
        bound = 0;
    } else {
        bound = colSize - startIndex;
    }

    auto rightExpandPath = std::make_unique<PathBuildExpression>();
    for (size_t i = 0; i < bound; ++i) {
        rightExpandPath->add(ExpressionUtils::inputPropExpr(colNames[i]));
    }

    auto leftExpandPath = std::make_unique<PathBuildExpression>();
    for (size_t i = bound; i < colNames.size(); ++i) {
        leftExpandPath->add(ExpressionUtils::inputPropExpr(colNames[i]));
    }

    auto finalPath = std::make_unique<PathBuildExpression>();
    if (leftExpandPath->size() != 0) {
        auto args = new ArgumentList();
        args->addArgument(std::move(leftExpandPath));
        auto reversePath =
            std::make_unique<FunctionCallExpression>(new std::string("reversePath"), args);
        if (rightExpandPath->size() == 0) {
            return new YieldColumn(reversePath.release(), new std::string(alias));
        }
        finalPath->add(std::move(reversePath));
    }
    if (rightExpandPath->size() != 0) {
        finalPath->add(std::move(rightExpandPath));
    }
    return new YieldColumn(finalPath.release(), new std::string(alias));
}

Status MatchClausePlanner::appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan) {
    if (matchClauseCtx->where == nullptr) {
        return Status::OK();
    }

    auto wherePlan = std::make_unique<WhereClausePlanner>()->transform(matchClauseCtx->where.get());
    NG_RETURN_IF_ERROR(wherePlan);
    auto plan = std::move(wherePlan).value();
    SegmentsConnector::addInput(plan.tail, subplan.root, true);
    subplan.root = plan.root;
    VLOG(1) << subplan;
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
