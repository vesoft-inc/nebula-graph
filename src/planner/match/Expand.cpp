/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/Expand.h"

#include "planner/Logic.h"
#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/SegmentsConnector.h"
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

std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> Expand::genEdgeProps(const EdgeInfo &edge) {
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    for (auto edgeType : edge.edgeTypes) {
        auto edgeSchema = matchCtx_->qctx->schemaMng()->getEdgeSchema(
            matchCtx_->space.id, edgeType);

        switch (edge.direction) {
            case Direction::OUT_EDGE: {
                if (reversely_) {
                    edgeType = -edgeType;
                }
                break;
            }
            case Direction::IN_EDGE: {
                if (!reversely_) {
                    edgeType = -edgeType;
                }
                break;
            }
            case Direction::BOTH: {
                EdgeProp edgeProp;
                edgeProp.set_type(-edgeType);
                std::vector<std::string> props{kSrc, kType, kRank, kDst};
                for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
                    props.emplace_back(edgeSchema->getFieldName(i));
                }
                edgeProp.set_props(std::move(props));
                edgeProps->emplace_back(std::move(edgeProp));
                break;
            }
        }
        EdgeProp edgeProp;
        edgeProp.set_type(edgeType);
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
            props.emplace_back(edgeSchema->getFieldName(i));
        }
        edgeProp.set_props(std::move(props));
        edgeProps->emplace_back(std::move(edgeProp));
    }
    return edgeProps;
}

static Expression* mergePathColumnsExpr(const std::string& lcol, const std::string& rcol) {
    auto expr = std::make_unique<PathBuildExpression>();
    expr->add(ExpressionUtils::inputPropExpr(lcol));
    expr->add(ExpressionUtils::inputPropExpr(rcol));
    return expr.release();
}

static Expression* buildPathExpr() {
    auto expr = std::make_unique<PathBuildExpression>();
    expr->add(std::make_unique<VertexExpression>());
    expr->add(std::make_unique<EdgeExpression>());
    return expr.release();
}

Status Expand::doExpand(const NodeInfo& node,
                        const EdgeInfo& edge,
                        SubPlan* plan) {
    NG_RETURN_IF_ERROR(expandSteps(node, edge, plan));
    NG_RETURN_IF_ERROR(filterDatasetByPathLength(edge, plan->root, plan));
    return Status::OK();
}

// (v)-[e]-()
// Status Expand::expandSteps(const NodeInfo& node,
//                            const EdgeInfo& edge,
//                            SubPlan* plan) {
//     SubPlan subplan;
//     int64_t startIndex = 0;
//     auto minHop = edge.range ? edge.range->min() : 1;
//     auto maxHop = edge.range ? edge.range->max() : 1;

//     // In the case of 0 step, src node is the dst node, return the vertex directly
//     if (minHop == 0) {
//         subplan = *plan;
//         startIndex = 0;
//         // Get vertex
//         NG_RETURN_IF_ERROR(MatchSolver::appendFetchVertexPlan(node.filter,
//                                                               matchCtx_->space,
//                                                               matchCtx_->qctx,
//                                                               initialExpr_.release(),
//                                                               inputVar_,
//                                                               subplan));
//         // If maxHop > 0, the result of 0 step will be passed to next plan node
//         if (maxHop > 0) {
//             subplan.root = passThrough(matchCtx_->qctx, subplan.root);
//         }
//     } else {  // Case 1 to n steps
//         startIndex = 1;
//         // Expand first step from src
//         NG_RETURN_IF_ERROR(expandStep(edge, dependency_, inputVar_, node.filter, &subplan));
//         // Manually create a passThrough node for the first step
//         // Rest steps will be passed through in collectData()
//         subplan.root = passThrough(matchCtx_->qctx, subplan.root);
//     }

//     PlanNode* passThrough = subplan.root;
//     for (; startIndex < maxHop; ++startIndex) {
//         SubPlan curr;
//         NG_RETURN_IF_ERROR(
//             expandStep(edge, passThrough, passThrough->outputVar(), nullptr, &curr)); //
//             curr.root is the result of project2
//         auto rNode = subplan.root;  // result of first step/Union
//         DCHECK(rNode->kind() == PNKind::kUnion || rNode->kind() == PNKind::kPassThrough);
//          // update passThrough, subplan.root is Union. passThrough is the result of start node
//         NG_RETURN_IF_ERROR(collectData(passThrough, curr.root, rNode, &passThrough, &subplan));
//     }
//     plan->root = subplan.root;

//     return Status::OK();
// }

// Build subplan: Project->Dedup->GetNeighbors->[Filter]->Project2->
// DataJoin->Project3->[Filter]->Passthrough->Union
Status Expand::expandSteps(const NodeInfo& node, const EdgeInfo& edge, SubPlan* plan) {
    SubPlan subplan;
    int64_t startIndex = 0;
    auto minHop = edge.range ? edge.range->min() : 1;
    auto maxHop = edge.range ? edge.range->max() : 1;

    // Build first step
    // In the case of 0 step, src node is the dst node, return the vertex directly
    if (minHop == 0) {
        subplan = *plan;
        startIndex = 0;
        // Get vertex
        NG_RETURN_IF_ERROR(MatchSolver::appendFetchVertexPlan(node.filter,
                                                              matchCtx_->space,
                                                              matchCtx_->qctx,
                                                              initialExpr_.release(),
                                                              inputVar_,
                                                              subplan));
        // If maxHop > 0, the result of 0 step will be passed to next plan node
        if (maxHop > 0) {
            subplan.root = passThrough(matchCtx_->qctx, subplan.root);
        } else {
            plan->root = subplan.root;
            return Status::OK();
        }
    } else {  // Case 1 to n steps
        startIndex = 1;
        // Expand first step from src
        NG_RETURN_IF_ERROR(expandStep(edge, dependency_, inputVar_, node.filter, &subplan));
        // Manually create a passThrough node for the first step
        // Rest steps will be passed to collectData()
        subplan.root = passThrough(matchCtx_->qctx, subplan.root);
    }
    // Result of first step expansion
    PlanNode* passThrough = subplan.root;


    // Build Start node from first step
    SubPlan loopBodyPlan;
    PlanNode* startNode = StartNode::make(matchCtx_->qctx);
    startNode->setOutputVar(passThrough->outputVar());
    startNode->setColNames({kPathStr});

    // Build node for Union
    PlanNode* rNode = new PlanNode(matchCtx_->qctx, PNKind::kUnion);
    rNode->setOutputVar(passThrough->outputVar());

    // Construct loop body
    NG_RETURN_IF_ERROR(expandStep(edge,
                                  startNode,                // dep
                                  startNode->outputVar(),   // inputVar
                                  nullptr,
                                  &loopBodyPlan));
    // auto rNode = subplan.root;

    DCHECK(rNode->kind() == PNKind::kUnion || rNode->kind() == PNKind::kPassThrough);

    NG_RETURN_IF_ERROR(collectData(startNode,           // left join node
                                   loopBodyPlan.root,   // right join node
                                   rNode,               // union node
                                   &startNode,
                                   &subplan));
    // Union node
    auto body = subplan.root;
    // loopBodyPlan.root= rNode; // Last node in loop body
    // loopBodyPlan.tail = startNode;

    // Loop condition
    auto condition = buildNStepLoopCondition(startIndex, maxHop);

    // Create loop
    auto* loop = Loop::make(matchCtx_->qctx, passThrough, body, condition);
    loop->setColNames({kPathStr});
    loop->setOutputVar(body->outputVar());

    // subplan.root = loop;

    plan->root = subplan.root;
    return Status::OK();
}

// Build subplan: Project->Dedup->GetNeighbors->[Filter]->Project
Status Expand::expandStep(const EdgeInfo& edge,
                          PlanNode* dep,
                          const std::string& inputVar,
                          const Expression* nodeFilter,
                          SubPlan* plan) {
    auto qctx = matchCtx_->qctx;

    // Extract dst vid from input project node which output dataset format is: [v1,e1,...,vn,en]
    SubPlan curr;
    curr.root = dep;
    MatchSolver::extractAndDedupVidColumn(qctx, initialExpr_.release(), dep, inputVar, curr);
    // [GetNeighbors]
    auto gn = GetNeighbors::make(qctx, curr.root, matchCtx_->space.id);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    gn->setSrc(qctx->objPool()->add(srcExpr.release()));
    gn->setVertexProps(genVertexProps());
    gn->setEdgeProps(genEdgeProps(edge));
    gn->setEdgeDirection(edge.direction);

    PlanNode* root = gn;
    // [Filter]
    if (nodeFilter != nullptr) {
        auto filter = qctx->objPool()->add(nodeFilter->clone().release());
        RewriteMatchLabelVisitor visitor(
            [](const Expression* expr) -> Expression *{
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute ||
                expr->kind() == Expression::Kind::kLabel);
            // filter prop
            if (expr->kind() == Expression::Kind::kLabelAttribute) {
                auto la = static_cast<const LabelAttributeExpression*>(expr);
                return new AttributeExpression(
                    new VertexExpression(), la->right()->clone().release());
            }
            // filter tag
            return new VertexExpression();
        });
        filter->accept(&visitor);
        auto filterNode = Filter::make(matchCtx_->qctx, root, filter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    if (edge.filter != nullptr) {
        RewriteMatchLabelVisitor visitor([](const Expression*expr) {
            DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
            auto la = static_cast<const LabelAttributeExpression*>(expr);
            return new AttributeExpression(new EdgeExpression(), la->right()->clone().release());
        });
        auto filter = saveObject(edge.filter->clone().release());
        filter->accept(&visitor);
        auto filterNode = Filter::make(qctx, root, filter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    auto listColumns = saveObject(new YieldColumns);
    listColumns->addColumn(new YieldColumn(buildPathExpr(), new std::string(kPathStr)));
    // [Project]
    root = Project::make(qctx, root, listColumns);
    root->setColNames({kPathStr});

    plan->root = root;
    plan->tail = curr.tail;
    return Status::OK();
}

// Build subplan: DataJoin->Project->[Filter]->Passthrough->Union
// In loop, start node and union node will be passed as joinLeft and inUnionNode
Status Expand::collectData(PlanNode* joinLeft,
                           const PlanNode* joinRight,
                           PlanNode* inUnionNode,
                           PlanNode** passThrough,
                           SubPlan* plan) {
    auto qctx = matchCtx_->qctx;
    // [dataJoin] read start node (joinLeft)
    auto join = SegmentsConnector::innerJoinSegments(qctx, joinLeft, joinRight);
    auto lpath = folly::stringPrintf("%s_%d", kPathStr, 0);
    auto rpath = folly::stringPrintf("%s_%d", kPathStr, 1);
    join->setColNames({lpath, rpath});
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergePathColumnsExpr(lpath, rpath);
    columns->addColumn(new YieldColumn(listExpr));
    // [Project]
    auto project = Project::make(qctx, join, columns);
    project->setColNames({kPathStr});
    // [Filter]
    auto filter = MatchSolver::filtPathHasSameEdge(project, kPathStr, qctx);
    // [Passthrough]
    auto pt = PassThroughNode::make(qctx, filter);
    pt->setOutputVar(filter->outputVar());
    pt->setColNames({kPathStr});
    // [Union]
    auto uNode = Union::make(qctx, pt, inUnionNode);
    uNode->setColNames({kPathStr});
    // Update Union node for next loop iteration
    inUnionNode->setOutputVar(uNode->outputVar());
    // Update start node
    joinLeft->setOutputVar(pt->outputVar());

    *passThrough = pt;
    plan->root = uNode;
    return Status::OK();
}

Status Expand::filterDatasetByPathLength(const EdgeInfo& edge,
                                         PlanNode* input,
                                         SubPlan* plan) {
    auto qctx = matchCtx_->qctx;

    // Filter rows whose edges number less than min hop
    auto args = std::make_unique<ArgumentList>();
    // Expr: length(relationships(p)) >= minHop
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
    // Plan->tail = curr.tail;
    return Status::OK();
}

PlanNode* Expand::passThrough(const QueryContext *qctx, const PlanNode *root) const {
    auto pt = PassThroughNode::make(const_cast<QueryContext*>(qctx), const_cast<PlanNode*>(root));
    pt->setOutputVar(root->outputVar());
    pt->setColNames(root->colNames());
    return pt;
}

Expression* Expand::buildNStepLoopCondition(int64_t startIndex, int64_t maxHop) const {
    VLOG(1) << "maxHop: " << maxHop;
    // ++loopSteps{0} < maxHop
    auto loopSteps = matchCtx_->qctx->vctx()->anonVarGen()->getVar();
    matchCtx_->qctx->ectx()->setValue(loopSteps, startIndex);
    return matchCtx_->qctx->objPool()->add(new RelationalExpression(
        Expression::Kind::kRelLT,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps), new ConstantExpression(0))),
        new ConstantExpression(static_cast<int64_t>(maxHop))));
}

}  // namespace graph
}  // namespace nebula
