/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIndexSeekPlanner.h"

#include "planner/Logic.h"
#include "planner/planners/MatchSolver.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

using nebula::storage::cpp2::EdgeProp;
using nebula::storage::cpp2::VertexProp;
using PNKind = nebula::graph::PlanNode::Kind;
using EdgeInfo = nebula::graph::EdgeInfo;
using NodeInfo = nebula::graph::MatchValidator::NodeInfo;

namespace nebula {
namespace graph {
bool MatchVertexIndexSeekPlanner::match(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    auto& head = matchCtx->nodeInfos[0];
    if (head.label == nullptr) {
        return false;
    }

    Expression *filter = nullptr;
    if (matchCtx->filter != nullptr) {
        filter = makeIndexFilter(*head.label, *head.alias, matchCtx->filter.get(), matchCtx->qctx);
    }
    if (filter == nullptr) {
        if (head.props != nullptr && !head.props->items().empty()) {
            filter = makeIndexFilter(*head.label, head.props, matchCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    matchCtx->scanInfo.filter = filter;
    matchCtx->scanInfo.schemaId = head.tid;

    return true;
}

Expression* MatchVertexIndexSeekPlanner::makeIndexFilter(const std::string &label,
                                                         const MapExpression *map,
                                                         QueryContext* qctx) {
    auto &items = map->items();
    Expression *root = new RelationalExpression(Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(label),
                new std::string(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
                new TagPropertyExpression(
                    new std::string(label),
                    new std::string(*items[i].first)),
                items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }
    return qctx->objPool()->add(root);
}

Expression* MatchVertexIndexSeekPlanner::makeIndexFilter(const std::string &label,
                                                         const std::string &alias,
                                                         Expression *filter,
                                                         QueryContext* qctx) {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE
    };

    std::vector<const Expression*> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        auto *logic = static_cast<LogicalExpression*>(filter);
        ExpressionUtils::pullAnds(logic);
        for (auto &operand : logic->operands()) {
            ands.emplace_back(operand.get());
        }
    } else {
        return nullptr;
    }

    std::vector<Expression*> relationals;
    for (auto *item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto *binary = static_cast<const BinaryExpression*>(item);
        auto *left = binary->left();
        auto *right = binary->right();
        const LabelAttributeExpression *la = nullptr;
        const ConstantExpression *constant = nullptr;
        if (left->kind() == Expression::Kind::kLabelAttribute &&
                right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(left);
            constant = static_cast<const ConstantExpression*>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(right);
            constant = static_cast<const ConstantExpression*>(left);
        } else {
            continue;
        }

        if (*la->left()->name() != alias) {
            continue;
        }

        auto *tpExpr = new TagPropertyExpression(
                new std::string(label),
                new std::string(*la->right()->name()));
        auto *newConstant = constant->clone().release();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto *rel = new RelationalExpression(item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto *rel = new RelationalExpression(item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto *root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto *left = root;
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, relationals[i]);
    }

    return qctx->objPool()->add(root);
}

StatusOr<SubPlan> MatchVertexIndexSeekPlanner::transform(AstContext* astCtx) {
    matchCtx_ = static_cast<MatchAstContext*>(astCtx);

    NG_RETURN_IF_ERROR(buildScanNode());
    if (!matchCtx_->edgeInfos.empty()) {
        NG_RETURN_IF_ERROR(buildSteps());
    }
    NG_RETURN_IF_ERROR(buildGetTailVertices());
    if (!matchCtx_->edgeInfos.empty()) {
        NG_RETURN_IF_ERROR(buildTailJoin());
    }
    NG_RETURN_IF_ERROR(buildFilter());
    NG_RETURN_IF_ERROR(MatchSolver::buildReturn(matchCtx_, subPlan_));
    return subPlan_;
}

Status MatchVertexIndexSeekPlanner::buildScanNode() {
    // FIXME: Move following validation to MatchValidator
    if (!startFromNode_) {
        return Status::SemanticError("Scan from edge not supported now");
    }
    if (startIndex_ != 0) {
        return Status::SemanticError("Only support scan from the head node");
    }

    using IQC = nebula::storage::cpp2::IndexQueryContext;
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back();
    contexts->back().set_filter(Expression::encode(*matchCtx_->scanInfo.filter));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(matchCtx_->qctx,
                                nullptr,
                                matchCtx_->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                matchCtx_->scanInfo.schemaId);
    subPlan_.tail = scan;
    subPlan_.root = scan;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildSteps() {
    gnSrcExpr_ = new VariablePropertyExpression(new std::string(),
                                                new std::string(kVid));
    saveObject(gnSrcExpr_);
    NG_RETURN_IF_ERROR(buildStep());
    for (auto i = 1u; i < matchCtx_->edgeInfos.size(); i++) {
        NG_RETURN_IF_ERROR(buildStep());
        NG_RETURN_IF_ERROR(buildStepJoin());
    }

    return Status::OK();
}

Status MatchVertexIndexSeekPlanner::buildStep() {
    curStep_++;

    auto &srcNodeInfo = matchCtx_->nodeInfos[curStep_];
    auto &edgeInfo = matchCtx_->edgeInfos[curStep_];
    auto *gn = GetNeighbors::make(matchCtx_->qctx, subPlan_.root, matchCtx_->space.id);
    gn->setSrc(gnSrcExpr_);
    auto vertexProps = std::make_unique<std::vector<VertexProp>>();
    if (srcNodeInfo.label != nullptr) {
        VertexProp vertexProp;
        vertexProp.set_tag(srcNodeInfo.tid);
        vertexProps->emplace_back(std::move(vertexProp));
    }
    gn->setVertexProps(std::move(vertexProps));
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (!edgeInfo.edgeTypes.empty()) {
        for (auto edgeType : edgeInfo.edgeTypes) {
            if (edgeInfo.direction == MatchValidator::Direction::IN_EDGE) {
                edgeType = -edgeType;
            } else if (edgeInfo.direction == MatchValidator::Direction::BOTH) {
                EdgeProp edgeProp;
                edgeProp.set_type(-edgeType);
                edgeProps->emplace_back(std::move(edgeProp));
            }
            EdgeProp edgeProp;
            edgeProp.set_type(edgeType);
            edgeProps->emplace_back(std::move(edgeProp));
        }
    }
    gn->setEdgeProps(std::move(edgeProps));
    gn->setEdgeDirection(edgeInfo.direction);

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    yields->addColumn(new YieldColumn(new EdgeExpression()));
    auto *project = Project::make(matchCtx_->qctx, gn, yields);
    project->setInputVar(gn->outputVar());
    project->setColNames({*srcNodeInfo.alias, *edgeInfo.alias});

    subPlan_.root = project;

    auto rewriter = [] (const Expression *expr) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
        return MatchSolver::rewrite(static_cast<const LabelAttributeExpression*>(expr));
    };

    if (srcNodeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        srcNodeInfo.filter->accept(&visitor);
        auto *node = Filter::make(matchCtx_->qctx, subPlan_.root, srcNodeInfo.filter);
        node->setInputVar(subPlan_.root->outputVar());
        node->setColNames(subPlan_.root->colNames());
        subPlan_.root = node;
    }

    if (edgeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        edgeInfo.filter->accept(&visitor);
        auto *node = Filter::make(matchCtx_->qctx, subPlan_.root, edgeInfo.filter);
        node->setInputVar(subPlan_.root->outputVar());
        node->setColNames(subPlan_.root->colNames());
        subPlan_.root = node;
    }

    gnSrcExpr_ = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(project->outputVar()),
                new std::string(*edgeInfo.alias)),
            new LabelExpression("_dst"));
    saveObject(gnSrcExpr_);

    prevStepRoot_ = thisStepRoot_;
    thisStepRoot_ = subPlan_.root;

    return Status::OK();
}

Status MatchVertexIndexSeekPlanner::buildGetTailVertices() {
    Expression *src = nullptr;
    if (!matchCtx_->edgeInfos.empty()) {
        src = new AttributeExpression(
                new VariablePropertyExpression(
                    new std::string(),
                    new std::string(*matchCtx_->edgeInfos[curStep_].alias)),
                new LabelExpression("_dst"));
    } else {
        src = new VariablePropertyExpression(new std::string(),
                new std::string(kVid));
    }
    saveObject(src);

    auto &nodeInfo = matchCtx_->nodeInfos[curStep_ + 1];
    std::vector<VertexProp> props;
    if (nodeInfo.label != nullptr) {
        VertexProp prop;
        prop.set_tag(nodeInfo.tid);
        props.emplace_back(prop);
    }

    auto *gv = GetVertices::make(
        matchCtx_->qctx, subPlan_.root, matchCtx_->space.id, src, std::move(props), {}, true);
    if (thisStepRoot_ != nullptr) {
        gv->setInputVar(thisStepRoot_->outputVar());
    }

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(matchCtx_->qctx, gv, yields);
    project->setInputVar(gv->outputVar());
    project->setColNames({*nodeInfo.alias});
    subPlan_.root = project;

    if (nodeInfo.filter != nullptr) {
        auto newFilter = nodeInfo.filter->clone();
        RewriteMatchLabelVisitor visitor([](auto *expr) {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                return MatchSolver::rewrite(static_cast<const LabelAttributeExpression*>(expr));
            });
        newFilter->accept(&visitor);
        auto *filter =
            Filter::make(matchCtx_->qctx, subPlan_.root, saveObject(newFilter.release()));
        filter->setInputVar(subPlan_.root->outputVar());
        filter->setColNames(subPlan_.root->colNames());
        subPlan_.root = filter;
    }

    auto *dedup = Dedup::make(matchCtx_->qctx, subPlan_.root);
    dedup->setInputVar(subPlan_.root->outputVar());
    dedup->setColNames(subPlan_.root->colNames());
    subPlan_.root = dedup;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildStepJoin() {
    auto prevStep = curStep_ - 1;
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(prevStepRoot_->outputVar()),
                new std::string(*matchCtx_->edgeInfos[prevStep].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*matchCtx_->nodeInfos[curStep_].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(matchCtx_->qctx,
                                subPlan_.root,
                                {prevStepRoot_->outputVar(), 0},
                                {thisStepRoot_->outputVar(), 0},
                                {key},
                                {probe});
    auto leftColNames = prevStepRoot_->colNames();
    auto rightColNames = thisStepRoot_->colNames();
    std::vector<std::string> colNames;
    colNames.reserve(leftColNames.size() + rightColNames.size());
    for (auto &name : leftColNames) {
        colNames.emplace_back(std::move(name));
    }
    for (auto &name : rightColNames) {
        colNames.emplace_back(std::move(name));
    }
    join->setColNames(std::move(colNames));
    subPlan_.root = join;
    thisStepRoot_ = subPlan_.root;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildTailJoin() {
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*matchCtx_->edgeInfos[curStep_].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(subPlan_.root->outputVar()),
                new std::string(*matchCtx_->nodeInfos[curStep_ + 1].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(matchCtx_->qctx,
                                subPlan_.root,
                                {thisStepRoot_->outputVar(), 0},
                                {subPlan_.root->outputVar(), 0},
                                {key},
                                {probe});
    auto colNames = thisStepRoot_->colNames();
    colNames.emplace_back(*matchCtx_->nodeInfos[curStep_ + 1].alias);
    join->setColNames(std::move(colNames));
    subPlan_.root = join;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildFilter() {
    if (matchCtx_->filter == nullptr) {
        return Status::OK();
    }
    auto newFilter = matchCtx_->filter->clone();
    auto rewriter = [this](const Expression *expr) {
        return MatchSolver::doRewrite(matchCtx_, expr);
    };
    RewriteMatchLabelVisitor visitor(std::move(rewriter));
    newFilter->accept(&visitor);

    auto *node = Filter::make(matchCtx_->qctx, subPlan_.root, saveObject(newFilter.release()));
    node->setInputVar(subPlan_.root->outputVar());
    node->setColNames(subPlan_.root->colNames());

    subPlan_.root = node;

    return Status::OK();
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

Status MatchVertexIndexSeekPlanner::composePlan(SubPlan *finalPlan) {
    auto &nodeInfos = matchCtx_->nodeInfos;
    auto &edgeInfos = matchCtx_->edgeInfos;
    if (edgeInfos.empty()) {
        DCHECK(!nodeInfos.empty());
        // FIXME(yee): only return source vertex
        return Status::OK();
    }
    DCHECK_GT(nodeInfos.size(), edgeInfos.size());

    // FIXME(yee): scan node subplan
    PlanNode *root = nullptr;
    SubPlan plan;
    NG_RETURN_IF_ERROR(filterFinalDataset(edgeInfos[0], root, &plan));
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

PlanNode *MatchVertexIndexSeekPlanner::joinDataSet(const PlanNode *right, const PlanNode *left) {
    auto leftKey = left->outputVar();
    auto rightKey = right->outputVar();
    auto buildExpr = getLastEdgeDstExprInFirstColumn(leftKey);
    auto probeExpr = getFirstVertexVidInFistColumn(rightKey);
    return DataJoin::make(matchCtx_->qctx,
                          const_cast<PlanNode *>(right),
                          {leftKey, 0},
                          {rightKey, 0},
                          {buildExpr},
                          {probeExpr});
}

Status MatchVertexIndexSeekPlanner::appendFetchVertexPlan(const PlanNode *input, SubPlan *plan) {
    auto qctx = matchCtx_->qctx;

    auto columns = saveObject(new YieldColumns);
    auto expr = getLastEdgeDstExprInLastColumn(input->outputVar());
    columns->addColumn(new YieldColumn(expr, new std::string()));
    auto project = Project::make(qctx, const_cast<PlanNode *>(input), columns);

    auto dedup = Dedup::make(qctx, project);

    auto srcExpr = ExpressionUtils::columnExpr(dedup->outputVar(), 0);
    // FIXME(yee): fill in expr for GV
    auto gv = GetVertices::make(qctx, dedup, matchCtx_->space.id, srcExpr, {}, {});

    // normalize all columns to one
    columns = saveObject(new YieldColumns);
    // FIXME(yee)
    Expression *listExpr = nullptr;
    columns->addColumn(new YieldColumn(listExpr, new std::string()));
    plan->root = Project::make(qctx, gv, columns);
}

Status MatchVertexIndexSeekPlanner::filterFinalDataset(const EdgeInfo &edge,
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
}

Status MatchVertexIndexSeekPlanner::composeSubPlan(const EdgeInfo &edge,
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
Status MatchVertexIndexSeekPlanner::expandStep(const EdgeInfo &edge,
                                               const PlanNode *input,
                                               SubPlan *plan) {
    DCHECK(input != nullptr);
    auto qctx = matchCtx_->qctx;
    auto colGen = qctx->vctx()->anonColGen();

    // Extract dst vid from input project node which output dataset format is: [v1,e1,...,vn,en]
    auto columns = saveObject(new YieldColumns);
    auto extractDstExpr = getLastEdgeDstExprInLastColumn(input->outputVar());
    columns->addColumn(new YieldColumn(extractDstExpr, new std::string(colGen->getCol())));
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
    auto listExpr = mergeColumnsExpr(root->outputVar(), 0, 1);
    listColumns->addColumn(new YieldColumn(listExpr, new std::string(colGen->getCol())));
    root = Project::make(qctx, root, listColumns);
    root->setColNames({"_path"});

    plan->root = root;
    plan->tail = project;
    return Status::OK();
}

Status MatchVertexIndexSeekPlanner::collectData(const PlanNode *joinLeft,
                                                const PlanNode *joinRight,
                                                const PlanNode *inUnionNode,
                                                PlanNode **passThrough,
                                                SubPlan *plan) {
    auto qctx = matchCtx_->qctx;
    auto join = joinDataSet(joinRight, joinLeft);
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergeColumnsExpr(join->outputVar(), 0, 1);
    columns->addColumn(new YieldColumn(listExpr, new std::string("_path")));
    auto project = Project::make(qctx, join, columns);

    *passThrough = PassThroughNode::make(qctx, project);
    (*passThrough)->setOutputVar(project->outputVar());
    (*passThrough)->setColNames(project->colNames());

    plan->root = Union::make(qctx, *passThrough, const_cast<PlanNode *>(inUnionNode));
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
