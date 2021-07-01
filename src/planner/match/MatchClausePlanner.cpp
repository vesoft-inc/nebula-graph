/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchClausePlanner.h"

#include "context/ast/CypherAstContext.h"
#include "planner/plan/Query.h"
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
                    initialExpr_ = edgeCtx.initialExpr->clone();
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

    // record the resolved node
    matchClauseCtx->leftExpandFilledNodeId.emplace(
        nodeInfos[startIndex].alias,
        std::pair(matchClausePlan.root, initialExpr_->clone()));
    matchClauseCtx->rightExpandFilledNodeId.emplace(
        nodeInfos[startIndex].alias,
        std::pair(matchClausePlan.root, initialExpr_->clone()));
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
    subplan.root = SegmentsConnector::innerJoinSegmentsWithExtra(
        matchClauseCtx->qctx, left, right, JoinStrategyPos::kStart, JoinStrategyPos::kStart,
        leftRightExpandJoinNodeId(matchClauseCtx, right, left, nodeInfos[startIndex].alias));
    return Status::OK();
}

Status MatchClausePlanner::leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                              const std::vector<EdgeInfo>& edgeInfos,
                                              MatchClauseContext* matchClauseCtx,
                                              size_t startIndex,
                                              std::string inputVar,
                                              SubPlan& subplan) {
    std::vector<std::string> joinColNames = {
        folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size() + startIndex)};
    for (size_t i = startIndex; i > 0; --i) {
        bool expandInto = matchClauseCtx->leftExpandFilledNodeId.find(nodeInfos[i-1].alias)
                          != matchClauseCtx->leftExpandFilledNodeId.end();
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx,
                                               i == startIndex ? initialExpr_->clone() : nullptr)
                          ->depends(subplan.root)
                          ->inputVar(inputVar)
                          ->colNumber(nodeInfos.size() + i)
                          ->reversely()
                          ->doExpand(nodeInfos[i], edgeInfos[i - 1], nodeInfos[i-1], &subplan);
        if (!status.ok()) {
            return status;
        }
        if (i < startIndex) {
            auto right = subplan.root;
            VLOG(1) << "left: " << folly::join(",", left->colNames())
                    << " right: " << folly::join(",", right->colNames());
            if (expandInto) {
                auto *dstId = leftExpandPreviousNodeId(matchClauseCtx,
                                                       nodeInfos[i-1].alias, left->outputVar());
                subplan.root = SegmentsConnector::innerJoinSegments(
                    matchClauseCtx->qctx, left, right, InnerJoinStrategy::JoinPos::kEnd,
                    InnerJoinStrategy::JoinPos::kStart, dstId);
            } else {
                subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx,
                                                                    left, right);
            }
            joinColNames.emplace_back(
                folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size() + i));
            subplan.root->setColNames(joinColNames);
        }
        inputVar = subplan.root->outputVar();
        // fill the expand node alias
        leftExpandFillNodeId(matchClauseCtx,
                   nodeInfos[i].alias,
                   subplan.root,
                   folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size() + i));
    }

    VLOG(1) << subplan;
    auto left = subplan.root;
    auto* initialExprCopy = initialExpr_->clone();
    NG_RETURN_IF_ERROR(
        MatchSolver::appendFetchVertexPlan(nodeInfos.front().filter,
                                           matchClauseCtx->space,
                                           matchClauseCtx->qctx,
                                           edgeInfos.empty() ? &initialExprCopy : nullptr,
                                           subplan));
    if (!edgeInfos.empty()) {
        auto right = subplan.root;
        VLOG(1) << "left: " << folly::join(",", left->colNames())
                << " right: " << folly::join(",", right->colNames());
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(
            folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size()));
        subplan.root->setColNames(joinColNames);
    }
    leftExpandFillNodeId(matchClauseCtx,
               nodeInfos.front().alias,
               subplan.root,
               folly::stringPrintf("%s_%lu", kPathStr, nodeInfos.size()));

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
        bool expandInto = matchClauseCtx->rightExpandFilledNodeId.find(nodeInfos[i+1].alias)
                          != matchClauseCtx->rightExpandFilledNodeId.end();
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx,
                                               i == startIndex ? initialExpr_->clone() : nullptr)
                          ->depends(subplan.root)
                          ->inputVar(subplan.root->outputVar())
                          ->colNumber(i)
                          ->doExpand(nodeInfos[i], edgeInfos[i], nodeInfos[i+1], &subplan);
        if (!status.ok()) {
            return status;
        }
        if (i > startIndex) {
            auto right = subplan.root;
            VLOG(1) << "left: " << folly::join(",", left->colNames())
                    << " right: " << folly::join(",", right->colNames());
            if (expandInto) {
                auto *dstId = rightExpandPreviousNodeId(matchClauseCtx,
                                                        nodeInfos[i+1].alias, left->outputVar());
                subplan.root = SegmentsConnector::innerJoinSegments(
                    matchClauseCtx->qctx, left, right, InnerJoinStrategy::JoinPos::kEnd,
                    InnerJoinStrategy::JoinPos::kStart, dstId);
            } else {
                subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx,
                                                                    left, right);
            }
            joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, i));
            subplan.root->setColNames(joinColNames);
        }
        // fill the expand node alias
        rightExpandFillNodeId(matchClauseCtx,
                   nodeInfos[i].alias,
                   subplan.root,
                   folly::stringPrintf("%s_%lu", kPathStr, i));
    }

    VLOG(1) << subplan;
    auto left = subplan.root;
    auto* initialExprCopy = initialExpr_->clone();
    NG_RETURN_IF_ERROR(
        MatchSolver::appendFetchVertexPlan(nodeInfos.back().filter,
                                           matchClauseCtx->space,
                                           matchClauseCtx->qctx,
                                           edgeInfos.empty() ? &initialExprCopy : nullptr,
                                           subplan));
    if (!edgeInfos.empty()) {
        auto right = subplan.root;
        VLOG(1) << "left: " << folly::join(",", left->colNames())
                << " right: " << folly::join(",", right->colNames());
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, edgeInfos.size()));
        subplan.root->setColNames(joinColNames);
    }
    rightExpandFillNodeId(matchClauseCtx,
              nodeInfos.back().alias,
              subplan.root,
              folly::stringPrintf("%s_%lu", kPathStr, edgeInfos.size()));

    VLOG(1) << subplan;
    return Status::OK();
}

Status MatchClausePlanner::expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                                          const std::vector<EdgeInfo>& edgeInfos,
                                          MatchClauseContext* matchClauseCtx,
                                          size_t startIndex,
                                          SubPlan& subplan) {
    return expandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
}

Status MatchClausePlanner::projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                                   size_t startIndex,
                                                   SubPlan& plan) {
    auto qctx = matchClauseCtx->qctx;
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto input = plan.root;
    const auto& inColNames = input->colNames();
    auto columns = qctx->objPool()->add(new YieldColumns);
    std::vector<std::string> colNames;

    auto addNode = [&, this](size_t i) {
        auto& nodeInfo = nodeInfos[i];
        if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
            if (i >= startIndex) {
                columns->addColumn(
                    buildVertexColumn(matchClauseCtx, inColNames[i - startIndex], nodeInfo.alias));
            } else if (startIndex == (nodeInfos.size() - 1)) {
                columns->addColumn(
                    buildVertexColumn(matchClauseCtx, inColNames[startIndex - i], nodeInfo.alias));
            } else {
                columns->addColumn(buildVertexColumn(
                    matchClauseCtx, inColNames[nodeInfos.size() - i], nodeInfo.alias));
            }
            colNames.emplace_back(nodeInfo.alias);
        }
    };

    for (size_t i = 0; i < edgeInfos.size(); i++) {
        VLOG(1) << "colSize: " << inColNames.size() << "i: " << i
                << " nodesize: " << nodeInfos.size() << " start: " << startIndex;
        addNode(i);
        auto& edgeInfo = edgeInfos[i];
        if (!edgeInfo.alias.empty() && !edgeInfo.anonymous) {
            if (i >= startIndex) {
                columns->addColumn(
                    buildEdgeColumn(matchClauseCtx, inColNames[i - startIndex], edgeInfo));
            } else if (startIndex == (nodeInfos.size() - 1)) {
                columns->addColumn(buildEdgeColumn(
                    matchClauseCtx, inColNames[edgeInfos.size() - 1 - i], edgeInfo));
            } else {
                columns->addColumn(
                    buildEdgeColumn(matchClauseCtx, inColNames[edgeInfos.size() - i], edgeInfo));
            }
            colNames.emplace_back(edgeInfo.alias);
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
    columns->addColumn(
        buildPathColumn(matchClauseCtx, alias, startIndex, inColNames, nodeInfos.size()));
    colNames.emplace_back(alias);

    auto project = Project::make(qctx, input, columns);
    project->setColNames(std::move(colNames));

    plan.root = MatchSolver::filtPathHasSameEdge(project, alias, qctx);
    VLOG(1) << plan;
    return Status::OK();
}

YieldColumn* MatchClausePlanner::buildVertexColumn(MatchClauseContext* matchClauseCtx,
                                                   const std::string& colName,
                                                   const std::string& alias) const {
    auto* pool = matchClauseCtx->qctx->objPool();
    auto colExpr = InputPropertyExpression::make(pool, colName);
    // startNode(path) => head node of path
    auto args = ArgumentList::make(pool);
    args->addArgument(colExpr);
    auto firstVertexExpr = FunctionCallExpression::make(pool, "startNode", args);
    return new YieldColumn(firstVertexExpr, alias);
}

YieldColumn* MatchClausePlanner::buildEdgeColumn(MatchClauseContext* matchClauseCtx,
                                                 const std::string& colName,
                                                 EdgeInfo& edge) const {
    auto* pool = matchClauseCtx->qctx->objPool();
    auto colExpr = InputPropertyExpression::make(pool, colName);
    // relationships(p)
    auto args = ArgumentList::make(pool);
    args->addArgument(colExpr);
    auto relExpr = FunctionCallExpression::make(pool, "relationships", args);
    Expression* expr = nullptr;
    if (edge.range != nullptr) {
        expr = relExpr;
    } else {
        // Get first edge in path list [e1, e2, ...]
        auto idxExpr = ConstantExpression::make(pool, 0);
        auto subExpr = SubscriptExpression::make(pool, relExpr, idxExpr);
        expr = subExpr;
    }
    return new YieldColumn(expr, edge.alias);
}

YieldColumn* MatchClausePlanner::buildPathColumn(MatchClauseContext* matchClauseCtx,
                                                 const std::string& alias,
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
    auto* pool = matchClauseCtx->qctx->objPool();
    auto rightExpandPath = PathBuildExpression::make(pool);
    for (size_t i = 0; i < bound; ++i) {
        rightExpandPath->add(InputPropertyExpression::make(pool, colNames[i]));
    }

    auto leftExpandPath = PathBuildExpression::make(pool);
    for (size_t i = bound; i < colNames.size(); ++i) {
        leftExpandPath->add(InputPropertyExpression::make(pool, colNames[i]));
    }

    auto finalPath = PathBuildExpression::make(pool);
    if (leftExpandPath->size() != 0) {
        auto args = ArgumentList::make(pool);
        args->addArgument(leftExpandPath);
        auto reversePath = FunctionCallExpression::make(pool, "reversePath", args);
        if (rightExpandPath->size() == 0) {
            return new YieldColumn(reversePath, alias);
        }
        finalPath->add(reversePath);
    }
    if (rightExpandPath->size() != 0) {
        finalPath->add(rightExpandPath);
    }
    return new YieldColumn(finalPath, alias);
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

void MatchClausePlanner::fillNodeId(ObjectPool *pool,
                                    FillNodeId &filledNodeId,
                                    const std::string &alias,
                                    const PlanNode *input,
                                    const std::string &colName) {
    // id(startNode($-.<colName>))
    auto pathExpr = InputPropertyExpression::make(pool, colName);
    auto *args = ArgumentList::make(pool, 1);
    args->addArgument(pathExpr);
    auto startNode = FunctionCallExpression::make(pool, "startNode", args);
    auto *idArgs = ArgumentList::make(pool, 1);
    idArgs->addArgument(std::move(startNode));
    auto id = FunctionCallExpression::make(pool, "id", idArgs);
    filledNodeId[alias] = std::pair(input, id);
}

Expression*
MatchClausePlanner::previousNodeId(const FillNodeId &filledNodeId,
                                   const std::string &dstNodeAlias,
                                   const std::string &inputVar) {
    auto find = filledNodeId.find(dstNodeAlias);
    DCHECK(find != filledNodeId.end());
    CHECK_EQ(inputVar, find->second.first->outputVar());
    return find->second.second->clone();
}

std::vector<std::pair<Expression*, Expression*>>
MatchClausePlanner::leftRightExpandJoinNodeId(const MatchClauseContext* matchClauseCtx,
                                              const PlanNode *left,
                                              const PlanNode *right,
                                              const std::string &midNodeAlias) {
    UNUSED(left);
    UNUSED(right);
    std::vector<std::pair<Expression*, Expression*>> result;
    for (const auto &l : matchClauseCtx->leftExpandFilledNodeId) {
        if (midNodeAlias == l.first) {
            continue;
        }
        const auto &find = matchClauseCtx->rightExpandFilledNodeId.find(l.first);
        if (find != matchClauseCtx->rightExpandFilledNodeId.end()) {
            DCHECK_EQ(left->outputVar(), l.second.first->outputVar());
            DCHECK_EQ(right->outputVar(), find->second.first->outputVar());
            result.emplace_back(find->second.second->clone(),
                                l.second.second->clone());
        }
    }
    return result;
}


}   // namespace graph
}   // namespace nebula
