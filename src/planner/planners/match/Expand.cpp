/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/Expand.h"

#include "planner/Logic.h"
#include "planner/Query.h"
#include "util/AnonColGenerator.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

using nebula::storage::cpp2::EdgeProp;
using nebula::storage::cpp2::VertexProp;
using PNKind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace graph {

static std::unique_ptr<std::vector<VertexProp>> genVertexProps() {
    return std::make_unique<std::vector<VertexProp>>();
}

static std::unique_ptr<std::vector<EdgeProp>> genEdgeProps(const EdgeInfo &edge) {
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (edge.edgeTypes.empty()) {
        return edgeProps;
    }

    for (auto edgeType : edge.edgeTypes) {
        if (edge.direction == Direction::IN_EDGE) {
            edgeType = -edgeType;
        } else if (edge.direction == Direction::BOTH) {
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
    // expr: startNode(path) => v1
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: v1[_vid] => vid
    return new AttributeExpression(firstVertexExpr.release(), new ConstantExpression(kVid));
}

Status Expand::doExpand(const NodeInfo &node,
                        const EdgeInfo &edge,
                        const PlanNode *input,
                        SubPlan *plan) {
    NG_RETURN_IF_ERROR(expandSteps(node, edge, input, plan));
    NG_RETURN_IF_ERROR(filterDatasetByPathLength(edge, plan->root, plan));
    return Status::OK();
}

Status Expand::expandSteps(const NodeInfo &node,
                           const EdgeInfo &edge,
                           const PlanNode *input,
                           SubPlan *plan) {
    SubPlan subplan;
    NG_RETURN_IF_ERROR(expandStep(edge, input, node.filter, true, &subplan));
    plan->tail = subplan.tail;
    PlanNode *passThrough = subplan.root;
    auto maxHop = edge.range ? edge.range->max() : 1;
    for (int64_t i = 1; i < maxHop; ++i) {
        SubPlan curr;
        NG_RETURN_IF_ERROR(expandStep(edge, passThrough, nullptr, false, &curr));
        auto rNode = subplan.root;
        DCHECK(rNode->kind() == PNKind::kUnion || rNode->kind() == PNKind::kPassThrough);
        NG_RETURN_IF_ERROR(collectData(passThrough, curr.root, rNode, &passThrough, &subplan));
    }
    plan->root = subplan.root;
    return Status::OK();
}

// build subplan: Project->Dedup->GetNeighbors->[Filter]->Project
Status Expand::expandStep(const EdgeInfo &edge,
                          const PlanNode *input,
                          const Expression *nodeFilter,
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

    if (nodeFilter != nullptr) {
        auto filter = nodeFilter->clone().release();
        RewriteMatchLabelVisitor visitor([](const Expression *expr) {
            DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
            auto la = static_cast<const LabelAttributeExpression *>(expr);
            auto attr = new LabelExpression(*la->right()->name());
            return new AttributeExpression(new VertexExpression(), attr);
        });
        filter->accept(&visitor);
        auto filterNode = Filter::make(matchCtx_->qctx, root, filter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    if (edge.filter != nullptr) {
        RewriteMatchLabelVisitor visitor([](const Expression *expr) {
            DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
            auto la = static_cast<const LabelAttributeExpression *>(expr);
            auto attr = new LabelExpression(*la->right()->name());
            return new AttributeExpression(new EdgeExpression(), attr);
        });
        auto filter = edge.filter->clone().release();
        filter->accept(&visitor);
        auto filterNode = Filter::make(qctx, root, filter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    auto listColumns = saveObject(new YieldColumns);
    listColumns->addColumn(new YieldColumn(buildPathExpr(), new std::string(kPathStr)));
    root = Project::make(qctx, root, listColumns);
    root->setColNames({kPathStr});

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

Status Expand::collectData(const PlanNode *joinLeft,
                           const PlanNode *joinRight,
                           const PlanNode *inUnionNode,
                           PlanNode **passThrough,
                           SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto join = joinDataSet(joinRight, joinLeft);
    auto lpath = folly::stringPrintf("%s_%d", kPathStr, 0);
    auto rpath = folly::stringPrintf("%s_%d", kPathStr, 1);
    join->setColNames({lpath, rpath});
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergePathColumnsExpr(lpath, rpath);
    columns->addColumn(new YieldColumn(listExpr));
    auto project = Project::make(qctx, join, columns);
    project->setColNames({kPathStr});

    auto filter = filterCyclePath(project, kPathStr);

    auto pt = PassThroughNode::make(qctx, filter);
    pt->setOutputVar(filter->outputVar());
    pt->setColNames({kPathStr});

    auto uNode = Union::make(qctx, pt, const_cast<PlanNode *>(inUnionNode));
    uNode->setColNames({kPathStr});

    *passThrough = pt;
    plan->root = uNode;
    return Status::OK();
}

Status Expand::filterDatasetByPathLength(const EdgeInfo &edge,
                                         PlanNode *input,
                                         SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    // filter rows whose edges number less than min hop
    auto args = std::make_unique<ArgumentList>();
    // expr: length(relationships(p)) >= minHop
    auto pathExpr = ExpressionUtils::inputPropExpr(kPathStr);
    args->addArgument(std::move(pathExpr));
    auto fn = std::make_unique<std::string>("length");
    auto edgeExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    auto minHop = edge.range == nullptr ? 1 : edge.range->min();
    auto minHopExpr = std::make_unique<ConstantExpression>(minHop);
    auto expr = std::make_unique<RelationalExpression>(
        Expression::Kind::kRelGE, edgeExpr.release(), minHopExpr.release());
    auto filter = Filter::make(qctx, input, saveObject(expr.release()));
    filter->setColNames(input->colNames());
    plan->root = filter;
    // plan->tail = curr.tail;
    return Status::OK();
}

PlanNode *Expand::filterCyclePath(PlanNode *input, const std::string &column) {
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(ExpressionUtils::inputPropExpr(column));
    auto fn = std::make_unique<std::string>("hasSameEdgeInPath");
    auto fnCall = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    auto falseConst = std::make_unique<ConstantExpression>(false);
    auto cond = std::make_unique<RelationalExpression>(
        Expression::Kind::kRelEQ, fnCall.release(), falseConst.release());
    auto filter = Filter::make(matchCtx_->qctx, input, saveObject(cond.release()));
    filter->setColNames(input->colNames());
    return filter;
}

void Expand::extractAndDedupVidColumn(SubPlan *plan) {
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

Expression *Expand::initialExprOrEdgeDstExpr(const PlanNode *node) {
    Expression *vidExpr = initialExpr_;
    if (vidExpr != nullptr) {
        initialExpr_ = nullptr;
    } else {
        vidExpr = getLastEdgeDstExprInLastPath(node->colNamesRef().back());
    }
    return vidExpr;
}

PlanNode *Expand::joinDataSet(const PlanNode *right, const PlanNode *left) {
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
}  // namespace graph
}  // namespace nebula
