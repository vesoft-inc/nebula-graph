/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVariableLengthPatternIndexScanPlanner.h"
#include <folly/String.h>

#include <memory>
#include <string>
#include <utility>

#include "common/base/Status.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/VertexExpression.h"
#include "parser/Clauses.h"
#include "parser/MatchSentence.h"
#include "planner/Logic.h"
#include "planner/Planner.h"
#include "planner/planners/MatchSolver.h"
#include "util/ExpressionUtils.h"
#include "validator/MatchValidator.h"
#include "visitor/RewriteMatchLabelVisitor.h"

using nebula::storage::cpp2::EdgeProp;
using nebula::storage::cpp2::VertexProp;
using PNKind = nebula::graph::PlanNode::Kind;
using EdgeInfo = nebula::graph::MatchValidator::EdgeInfo;
using NodeInfo = nebula::graph::MatchValidator::NodeInfo;

namespace nebula {
namespace graph {

std::unique_ptr<MatchVariableLengthPatternIndexScanPlanner>
MatchVariableLengthPatternIndexScanPlanner::make() {
    return std::unique_ptr<MatchVariableLengthPatternIndexScanPlanner>(
        new MatchVariableLengthPatternIndexScanPlanner());
}

bool MatchVariableLengthPatternIndexScanPlanner::match(AstContext *astCtx) {
    return MatchSolver::match(astCtx);
}

StatusOr<SubPlan> MatchVariableLengthPatternIndexScanPlanner::transform(AstContext *astCtx) {
    matchCtx_ = static_cast<MatchAstContext *>(astCtx);
    SubPlan plan;
    NG_RETURN_IF_ERROR(scanIndex(&plan));
    NG_RETURN_IF_ERROR(combinePlans(&plan));
    NG_RETURN_IF_ERROR(projectColumnsBySymbols(&plan));
    NG_RETURN_IF_ERROR(MatchSolver::buildFilter(matchCtx_, &plan));
    NG_RETURN_IF_ERROR(MatchSolver::buildReturn(matchCtx_, plan));
    return plan;
}

static std::unique_ptr<std::vector<VertexProp>> genVertexProps() {
    return std::make_unique<std::vector<VertexProp>>();
}

static std::unique_ptr<std::vector<EdgeProp>> genEdgeProps(const EdgeInfo &edge) {
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (edge.edgeTypes.empty()) {
        return edgeProps;
    }

    for (auto edgeType : edge.edgeTypes) {
        if (edge.direction == MatchValidator::Direction::IN_EDGE) {
            edgeType = -edgeType;
        } else if (edge.direction == MatchValidator::Direction::BOTH) {
            EdgeProp edgeProp;
            edgeProp.set_type(-edgeType);
            edgeProps->emplace_back(std::move(edgeProp));
        }
        EdgeProp edgeProp;
        edgeProp.set_type(edgeType);
        edgeProps->emplace_back(std::move(edgeProp));
    }
    return edgeProps;
}

static Expression *getLastEdgeDstExprInLastPath(const std::string &colName) {
    // expr: __Project_2[-1] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: endNode(path) => vn
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("endNode");
    auto endNode = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: en[_dst] => dst vid
    auto vidExpr = std::make_unique<ConstantExpression>(kVid);
    return new AttributeExpression(endNode.release(), vidExpr.release());
}

static Expression *getFirstVertexVidInFistPath(const std::string &colName) {
    // expr: __Project_2[0] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: [v1, e1, ..., vn, en][0] => v1
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: v1[_vid] => vid
    return new AttributeExpression(firstVertexExpr.release(), new ConstantExpression(kVid));
}

static Expression *mergePathColumnsExpr(const std::string &lcol, const std::string &rcol) {
    auto expr = std::make_unique<PathBuildExpression>();
    expr->add(ExpressionUtils::inputPropExpr(lcol));
    expr->add(ExpressionUtils::inputPropExpr(rcol));
    return expr.release();
}

static Expression *buildPathExpr() {
    auto expr = std::make_unique<PathBuildExpression>();
    expr->add(std::make_unique<VertexExpression>());
    expr->add(std::make_unique<EdgeExpression>());
    return expr.release();
}

Status MatchVariableLengthPatternIndexScanPlanner::combinePlans(SubPlan *finalPlan) {
    auto &nodeInfos = matchCtx_->nodeInfos;
    auto &edgeInfos = matchCtx_->edgeInfos;
    DCHECK(!nodeInfos.empty());
    if (edgeInfos.empty()) {
        return appendFetchVertexPlan(finalPlan);
    }
    DCHECK_GT(nodeInfos.size(), edgeInfos.size());

    SubPlan plan;
    NG_RETURN_IF_ERROR(filterDatasetByPathLength(edgeInfos[0], finalPlan->root, &plan));
    std::vector<std::string> joinColNames = {folly::stringPrintf("%s_%d", kPath, 0)};
    for (size_t i = 1; i < edgeInfos.size(); ++i) {
        SubPlan curr;
        NG_RETURN_IF_ERROR(filterDatasetByPathLength(edgeInfos[i], plan.root, &curr));
        plan.root = joinDataSet(curr.root, plan.root);
        joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPath, i));
        plan.root->setColNames(joinColNames);
    }

    auto left = plan.root;
    NG_RETURN_IF_ERROR(appendFetchVertexPlan(&plan));
    finalPlan->root = joinDataSet(plan.root, left);
    joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPath, edgeInfos.size()));
    finalPlan->root->setColNames(joinColNames);

    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::projectColumnsBySymbols(SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto &nodeInfos = matchCtx_->nodeInfos;
    auto &edgeInfos = matchCtx_->edgeInfos;
    auto columns = saveObject(new YieldColumns);
    auto input = plan->root;
    std::vector<std::string> colNames;
    for (size_t i = 0; i < edgeInfos.size(); i++) {
        auto &nodeInfo = nodeInfos[i];
        if (nodeInfo.alias != nullptr && !nodeInfo.anonymous) {
            columns->addColumn(buildVertexColumn(i));
            colNames.emplace_back(*nodeInfo.alias);
        }
        auto &edgeInfo = edgeInfos[i];
        if (edgeInfo.alias != nullptr && !edgeInfo.anonymous) {
            columns->addColumn(buildEdgeColumn(i));
            colNames.emplace_back(*edgeInfo.alias);
        }
    }
    for (auto &alias : matchCtx_->aliases) {
        if (alias.second == MatchValidator::AliasType::kPath) {
            columns->addColumn(buildPathColumn(alias.first));
            colNames.emplace_back(alias.first);
        }
    }

    auto project = Project::make(qctx, input, columns);
    project->setColNames(std::move(colNames));

    plan->root = project;
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::scanIndex(SubPlan *plan) {
    using IQC = nebula::storage::cpp2::IndexQueryContext;
    IQC iqctx;
    iqctx.set_filter(Expression::encode(*matchCtx_->scanInfo.filter));
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back(std::move(iqctx));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(matchCtx_->qctx,
                                nullptr,
                                matchCtx_->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                matchCtx_->scanInfo.schemaId);
    plan->tail = scan;
    plan->root = scan;

    // initialize start expression in project node
    initialExpr_ = ExpressionUtils::newVarPropExpr(kVid);

    return Status::OK();
}

PlanNode *MatchVariableLengthPatternIndexScanPlanner::joinDataSet(const PlanNode *right,
                                                                  const PlanNode *left) {
    auto &leftKey = left->colNamesRef().back();
    auto &rightKey = right->colNamesRef().front();
    auto buildExpr = getLastEdgeDstExprInLastPath(leftKey);
    auto probeExpr = getFirstVertexVidInFistPath(rightKey);
    auto join = DataJoin::make(matchCtx_->qctx,
                               const_cast<PlanNode *>(right),
                               {left->outputVar(), 0},
                               {right->outputVar(), 0},
                               {buildExpr},
                               {probeExpr});
    std::vector<std::string> colNames = left->colNames();
    const auto &rightColNames = right->colNamesRef();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}

Status MatchVariableLengthPatternIndexScanPlanner::appendFetchVertexPlan(SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    extractAndDedupVidColumn(plan);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    auto gv = GetVertices::make(qctx, plan->root, matchCtx_->space.id, srcExpr.release(), {}, {});

    // normalize all columns to one
    auto columns = saveObject(new YieldColumns);
    auto pathExpr = std::make_unique<PathBuildExpression>();
    pathExpr->add(std::make_unique<VertexExpression>());
    columns->addColumn(new YieldColumn(pathExpr.release()));
    plan->root = Project::make(qctx, gv, columns);
    plan->root->setColNames({kPath});
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::filterDatasetByPathLength(const EdgeInfo &edge,
                                                                             const PlanNode *input,
                                                                             SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    SubPlan curr;
    NG_RETURN_IF_ERROR(combineSubPlan(edge, input, &curr));
    // filter rows whose edges number less than min hop
    auto args = std::make_unique<ArgumentList>();
    // expr: length(relationships(p)) >= minHop
    auto pathExpr = ExpressionUtils::inputPropExpr(kPath);
    args->addArgument(std::move(pathExpr));
    auto fn = std::make_unique<std::string>("length");
    auto edgeExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    auto minHop = edge.range == nullptr ? 1 : edge.range->min();
    auto minHopExpr = std::make_unique<ConstantExpression>(minHop);
    auto expr = std::make_unique<RelationalExpression>(
        Expression::Kind::kRelGE, edgeExpr.release(), minHopExpr.release());
    auto filter = Filter::make(qctx, curr.root, saveObject(expr.release()));
    filter->setColNames(curr.root->colNames());
    plan->root = filter;
    plan->tail = curr.tail;
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::combineSubPlan(const EdgeInfo &edge,
                                                                  const PlanNode *input,
                                                                  SubPlan *plan) {
    SubPlan subplan;
    NG_RETURN_IF_ERROR(expandStep(edge, input, true, &subplan));
    plan->tail = subplan.tail;
    PlanNode *passThrough = subplan.root;
    auto maxHop = edge.range ? edge.range->max() : 1;
    for (int64_t i = 1; i < maxHop; ++i) {
        SubPlan curr;
        NG_RETURN_IF_ERROR(expandStep(edge, passThrough, false, &curr));
        auto rNode = subplan.root;
        DCHECK(rNode->kind() == PNKind::kUnion || rNode->kind() == PNKind::kPassThrough);
        NG_RETURN_IF_ERROR(collectData(passThrough, curr.root, rNode, &passThrough, &subplan));
    }
    plan->root = subplan.root;
    return Status::OK();
}

// build subplan: Project->Dedup->GetNeighbors->[Filter]->Project
Status MatchVariableLengthPatternIndexScanPlanner::expandStep(const EdgeInfo &edge,
                                                              const PlanNode *input,
                                                              bool needPassThrough,
                                                              SubPlan *plan) {
    DCHECK(input != nullptr);
    auto qctx = matchCtx_->qctx;

    // Extract dst vid from input project node which output dataset format is: [v1,e1,...,vn,en]
    SubPlan curr;
    curr.root = const_cast<PlanNode *>(input);
    extractAndDedupVidColumn(&curr);

    auto gn = GetNeighbors::make(qctx, curr.root, matchCtx_->space.id);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    gn->setSrc(srcExpr.release());
    gn->setVertexProps(genVertexProps());
    gn->setEdgeProps(genEdgeProps(edge));
    gn->setEdgeDirection(edge.direction);

    PlanNode *root = gn;
    if (edge.filter != nullptr) {
        RewriteMatchLabelVisitor visitor([](const Expression *expr) {
            DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
            auto la = static_cast<const LabelAttributeExpression *>(expr);
            auto attr = new LabelExpression(*la->right()->name());
            return new AttributeExpression(new EdgeExpression(), attr);
        });
        auto filter = edge.filter->clone().release();
        filter->accept(&visitor);
        root = Filter::make(qctx, root, filter);
    }

    auto listColumns = saveObject(new YieldColumns);
    listColumns->addColumn(new YieldColumn(buildPathExpr(), new std::string(kPath)));
    root = Project::make(qctx, root, listColumns);
    root->setColNames({kPath});

    if (needPassThrough) {
        auto pt = PassThroughNode::make(qctx, root);
        pt->setColNames(root->colNames());
        pt->setOutputVar(root->outputVar());
        root = pt;
    }

    plan->root = root;
    plan->tail = curr.tail;
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::collectData(const PlanNode *joinLeft,
                                                               const PlanNode *joinRight,
                                                               const PlanNode *inUnionNode,
                                                               PlanNode **passThrough,
                                                               SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto join = joinDataSet(joinRight, joinLeft);
    auto lpath = folly::stringPrintf("%s_%d", kPath, 0);
    auto rpath = folly::stringPrintf("%s_%d", kPath, 1);
    join->setColNames({lpath, rpath});
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergePathColumnsExpr(lpath, rpath);
    columns->addColumn(new YieldColumn(listExpr));
    auto project = Project::make(qctx, join, columns);
    project->setColNames({kPath});

    auto pt = PassThroughNode::make(qctx, project);
    pt->setOutputVar(project->outputVar());
    pt->setColNames({kPath});

    auto uNode = Union::make(qctx, pt, const_cast<PlanNode *>(inUnionNode));
    uNode->setColNames({kPath});

    *passThrough = pt;
    plan->root = uNode;
    return Status::OK();
}

Expression *MatchVariableLengthPatternIndexScanPlanner::initialExprOrEdgeDstExpr(
    const PlanNode *node) {
    Expression *vidExpr = initialExpr_;
    if (vidExpr != nullptr) {
        initialExpr_ = nullptr;
    } else {
        vidExpr = getLastEdgeDstExprInLastPath(node->colNamesRef().back());
    }
    return vidExpr;
}

void MatchVariableLengthPatternIndexScanPlanner::extractAndDedupVidColumn(SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto columns = saveObject(new YieldColumns);
    auto input = plan->root;
    Expression *vidExpr = initialExprOrEdgeDstExpr(input);
    columns->addColumn(new YieldColumn(vidExpr));
    auto project = Project::make(qctx, input, columns);
    project->setColNames({kVid});
    auto dedup = Dedup::make(qctx, project);
    dedup->setColNames({kVid});

    plan->root = dedup;
}

YieldColumn *MatchVariableLengthPatternIndexScanPlanner::buildVertexColumn(int colIdx) const {
    auto colExpr = getNthPathExpr(colIdx);
    // startNode(path) => head node of path
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    auto alias = matchCtx_->nodeInfos[colIdx].alias;
    return new YieldColumn(firstVertexExpr.release(), new std::string(*alias));
}

YieldColumn *MatchVariableLengthPatternIndexScanPlanner::buildEdgeColumn(int colIdx) const {
    auto &edge = matchCtx_->edgeInfos[colIdx];
    auto colExpr = getNthPathExpr(colIdx);
    // relationships(p)
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("relationships");
    auto relExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    Expression *expr = nullptr;
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

YieldColumn *MatchVariableLengthPatternIndexScanPlanner::buildPathColumn(
    const std::string &alias) const {
    auto &nodeInfos = matchCtx_->nodeInfos;
    auto &edgeInfos = matchCtx_->edgeInfos;
    auto pathExpr = std::make_unique<PathBuildExpression>();
    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        auto colExpr = getNthPathExpr(i);
        pathExpr->add(std::move(colExpr));
    }
    auto colExpr = getNthPathExpr(nodeInfos.size() - 1);
    pathExpr->add(std::move(colExpr));
    return new YieldColumn(pathExpr.release(), new std::string(alias));
}

std::unique_ptr<Expression> MatchVariableLengthPatternIndexScanPlanner::getNthPathExpr(
    size_t colIdx) const {
    auto colName = folly::stringPrintf("%s_%lu", kPath, colIdx);
    return ExpressionUtils::inputPropExpr(colName);
}

}   // namespace graph
}   // namespace nebula
