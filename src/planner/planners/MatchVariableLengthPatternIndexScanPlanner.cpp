/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVariableLengthPatternIndexScanPlanner.h"

#include "planner/Logic.h"
#include "planner/planners/MatchSolver.h"
#include "util/ExpressionUtils.h"

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
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto *matchCtx = static_cast<MatchAstContext *>(astCtx);

    auto &head = matchCtx->nodeInfos[0];
    if (head.label == nullptr) {
        return false;
    }

    Expression *filter = nullptr;
    if (matchCtx->filter != nullptr) {
        filter = MatchSolver::makeIndexFilter(
            *head.label, *head.alias, matchCtx->filter.get(), matchCtx->qctx);
    }
    if (filter == nullptr) {
        if (head.props != nullptr && !head.props->items().empty()) {
            filter = MatchSolver::makeIndexFilter(*head.label, head.props, matchCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    matchCtx->scanInfo.filter = filter;
    matchCtx->scanInfo.schemaId = head.tid;

    return true;
}

StatusOr<SubPlan> MatchVariableLengthPatternIndexScanPlanner::transform(AstContext *astCtx) {
    matchCtx_ = static_cast<MatchAstContext *>(astCtx);
    SubPlan plan;
    NG_RETURN_IF_ERROR(scanIndex(&plan));
    NG_RETURN_IF_ERROR(composePlan(&plan));
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

static Expression *getLastEdgeDstExprInLastColumn(const std::string &varname) {
    // expr: __Project_2[-1] => [v1, e1,..., vn, en]
    auto columnExpr = ExpressionUtils::columnExpr(varname, -1);
    // expr: [v1, e1, ..., vn, en][-1] => en
    auto lastEdgeExpr = new SubscriptExpression(columnExpr, new ConstantExpression(-1));
    // expr: en[_dst] => dst vid
    return new AttributeExpression(lastEdgeExpr, new ConstantExpression(kDst));
}

static Expression *getFirstVertexVidInFistColumn(const std::string &varname) {
    // expr: __Project_2[0] => [v1, e1,..., vn, en]
    auto columnExpr = ExpressionUtils::columnExpr(varname, 0);
    // expr: [v1, e1, ..., vn, en][-1] => v1
    auto firstVertexExpr = new SubscriptExpression(columnExpr, new ConstantExpression(0));
    // expr: v1[_vid] => vid
    return new AttributeExpression(firstVertexExpr, new ConstantExpression(kVid));
}

static Expression *mergeColumnsExpr(const std::string &varname, int col1Idx, int col2Idx) {
    auto listItems = std::make_unique<ExpressionList>();
    listItems->add(ExpressionUtils::columnExpr(varname, col1Idx));
    listItems->add(ExpressionUtils::columnExpr(varname, col2Idx));
    return new ListExpression(listItems.release());
}

static Expression *mergeVertexAndEdgeExpr() {
    auto listItems = std::make_unique<ExpressionList>();
    listItems->add(new VertexExpression());
    listItems->add(new EdgeExpression());
    return new ListExpression(listItems.release());
}

Status MatchVariableLengthPatternIndexScanPlanner::composePlan(SubPlan *finalPlan) {
    auto &nodeInfos = matchCtx_->nodeInfos;
    auto &edgeInfos = matchCtx_->edgeInfos;
    DCHECK(!nodeInfos.empty());
    if (edgeInfos.empty()) {
        return appendFetchVertexPlan(finalPlan->root, finalPlan);
    }
    DCHECK_GT(nodeInfos.size(), edgeInfos.size());

    SubPlan plan;
    NG_RETURN_IF_ERROR(filterFinalDataset(edgeInfos[0], finalPlan->root, &plan));
    finalPlan->tail = plan.tail;
    for (size_t i = 1; i < edgeInfos.size(); ++i) {
        SubPlan curr;
        NG_RETURN_IF_ERROR(filterFinalDataset(edgeInfos[i], plan.root, &curr));
        plan.root = joinDataSet(curr.root, plan.root);
    }

    SubPlan curr;
    NG_RETURN_IF_ERROR(appendFetchVertexPlan(plan.root, &curr));
    finalPlan->root = joinDataSet(curr.root, plan.root);

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
    auto leftKey = left->outputVar();
    auto rightKey = right->outputVar();
    auto buildExpr = getLastEdgeDstExprInLastColumn(leftKey);
    auto probeExpr = getFirstVertexVidInFistColumn(rightKey);
    return DataJoin::make(matchCtx_->qctx,
                          const_cast<PlanNode *>(right),
                          {leftKey, 0},
                          {rightKey, 0},
                          {buildExpr},
                          {probeExpr});
}

Status MatchVariableLengthPatternIndexScanPlanner::appendFetchVertexPlan(const PlanNode *input,
                                                                         SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    auto columns = saveObject(new YieldColumns);
    auto expr = initialExprOrEdgeDstExpr(input->outputVar());
    columns->addColumn(new YieldColumn(expr));
    auto project = Project::make(qctx, const_cast<PlanNode *>(input), columns);

    auto dedup = Dedup::make(qctx, project);

    auto srcExpr = ExpressionUtils::columnExpr(dedup->outputVar(), -1);
    auto gv = GetVertices::make(qctx, dedup, matchCtx_->space.id, srcExpr, {}, {});

    // normalize all columns to one
    columns = saveObject(new YieldColumns);
    auto items = new ExpressionList();
    items->add(new VertexExpression());
    auto listExpr = new ListExpression(items);
    columns->addColumn(new YieldColumn(listExpr));
    plan->root = Project::make(qctx, gv, columns);
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::filterFinalDataset(const EdgeInfo &edge,
                                                                      const PlanNode *input,
                                                                      SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    SubPlan curr;
    NG_RETURN_IF_ERROR(composeSubPlan(edge, input, &curr));
    // filter rows whose edges number less than min hop
    auto args = new ArgumentList;
    auto listExpr = ExpressionUtils::columnExpr(input->outputVar(), 0);
    args->addArgument(std::unique_ptr<Expression>(listExpr));
    auto edgeExpr = new FunctionCallExpression(new std::string("size"), args);
    auto minHopExpr = new ConstantExpression(2 * edge.minHop);
    auto expr = new RelationalExpression(Expression::Kind::kRelGE, edgeExpr, minHopExpr);
    saveObject(expr);
    auto filter = Filter::make(qctx, curr.root, expr);

    plan->root = PassThroughNode::make(qctx, filter);
    plan->tail = curr.tail;
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::composeSubPlan(const EdgeInfo &edge,
                                                                  const PlanNode *input,
                                                                  SubPlan *plan) {
    SubPlan subplan;
    NG_RETURN_IF_ERROR(expandStep(edge, input, &subplan));
    plan->tail = subplan.tail;
    PlanNode *passThrough = subplan.root;
    for (int64_t i = 1; i <= edge.maxHop; ++i) {
        SubPlan curr;
        NG_RETURN_IF_ERROR(expandStep(edge, passThrough, &curr));
        auto rNode = subplan.root;
        DCHECK(rNode->kind() == PNKind::kUnion || rNode->kind() == PNKind::kProject);
        NG_RETURN_IF_ERROR(collectData(passThrough, curr.root, rNode, &passThrough, &subplan));
    }
    plan->root = subplan.root;
    return Status::OK();
}

// build subplan: Project->Dedup->GetNeighbors->[Filter]->Project
Status MatchVariableLengthPatternIndexScanPlanner::expandStep(const EdgeInfo &edge,
                                                              const PlanNode *input,
                                                              SubPlan *plan) {
    DCHECK(input != nullptr);
    auto qctx = matchCtx_->qctx;

    // Extract dst vid from input project node which output dataset format is: [v1,e1,...,vn,en]
    auto columns = saveObject(new YieldColumns);
    Expression *vidExpr = initialExprOrEdgeDstExpr(input->outputVar());
    columns->addColumn(new YieldColumn(vidExpr));
    auto project = Project::make(qctx, const_cast<PlanNode *>(input), columns);

    auto dedup = Dedup::make(qctx, project);

    auto gn = GetNeighbors::make(qctx, dedup, matchCtx_->space.id);
    gn->setSrc(ExpressionUtils::columnExpr(dedup->outputVar(), 0));
    gn->setVertexProps(genVertexProps());
    gn->setEdgeProps(genEdgeProps(edge));
    gn->setEdgeDirection(edge.direction);

    PlanNode *root = gn;
    if (edge.filter != nullptr) {
        // FIXME
        root = Filter::make(qctx, root, edge.filter);
    }

    auto listColumns = saveObject(new YieldColumns);
    listColumns->addColumn(new YieldColumn(mergeVertexAndEdgeExpr()));
    root = Project::make(qctx, root, listColumns);
    root->setColNames({"_path"});

    plan->root = root;
    plan->tail = project;
    return Status::OK();
}

Status MatchVariableLengthPatternIndexScanPlanner::collectData(const PlanNode *joinLeft,
                                                               const PlanNode *joinRight,
                                                               const PlanNode *inUnionNode,
                                                               PlanNode **passThrough,
                                                               SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto join = joinDataSet(joinRight, joinLeft);
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergeColumnsExpr(join->outputVar(), 0, 1);
    columns->addColumn(new YieldColumn(listExpr));
    auto project = Project::make(qctx, join, columns);
    project->setColNames({"_path"});

    *passThrough = PassThroughNode::make(qctx, project);
    (*passThrough)->setOutputVar(project->outputVar());
    (*passThrough)->setColNames(project->colNames());

    plan->root = Union::make(qctx, *passThrough, const_cast<PlanNode *>(inUnionNode));
    return Status::OK();
}

Expression *MatchVariableLengthPatternIndexScanPlanner::initialExprOrEdgeDstExpr(
    const std::string &varname) {
    Expression *vidExpr = initialExpr_;
    if (vidExpr != nullptr) {
        initialExpr_ = nullptr;
    } else {
        vidExpr = getLastEdgeDstExprInLastColumn(varname);
    }
    return vidExpr;
}

}   // namespace graph
}   // namespace nebula
