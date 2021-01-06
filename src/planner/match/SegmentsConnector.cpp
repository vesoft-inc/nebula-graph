/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/SegmentsConnector.h"
#include "planner/match/AddDependencyStrategy.h"
#include "planner/match/AddInputStrategy.h"
#include "planner/match/InnerJoinStrategy.h"
#include "planner/match/CartesianProductStrategy.h"
#include "planner/match/ApplyStrategy.h"
#include "planner/Logic.h"
#include "planner/Query.h"
#include "common/expression/VariableExpression.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

StatusOr<SubPlan> SegmentsConnector::connectSegments(CypherClauseContextBase* leftCtx,
                                                     CypherClauseContextBase* rightCtx,
                                                     SubPlan& left,
                                                     SubPlan& right,
                                                     QueryContext* qctx) {
    if (leftCtx->kind == CypherClauseKind::kReturn) {
        VLOG(1) << "left tail: " << left.tail->outputVar()
                << "right root: " << right.root->outputVar();
        addInput(left.tail, right.root);
        left.tail = right.tail;
        return left;
    } else if (leftCtx->kind == CypherClauseKind::kMatch &&
               rightCtx->kind == CypherClauseKind::kUnwind) {
        VLOG(1) << "left tail: " << left.tail->outputVar()
                << "right root: " << right.root->outputVar();
        auto* start = StartNode::make(qctx);

    const auto& rowIndex = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(rowIndex, Value(-1));

        rewriteMatchClause(
            qctx, static_cast<MatchClauseContext*>(leftCtx), left, right.root, rowIndex);

        auto* iterate = iterateDataSet(qctx, right.root, rowIndex);
        addInput(iterate, start);
        addInput(left.tail, iterate);
        left.tail = start;

        auto* cartesianProduct = cartesianProductSegments(qctx, left.root, iterate);
        left.root = cartesianProduct;
        auto* trans = transformDataSet(qctx, left.root);
        left.root = trans;

        auto* apply = applySegments(qctx, left.root, right.root, rowIndex);
        DCHECK(apply != nullptr);
        left.root = apply;
        left.tail = right.tail;
        return left;
    } else if (leftCtx->kind == CypherClauseKind::kMatch) {
        VLOG(1) << "left tail: " << left.tail->outputVar()
                << "right root: " << right.root->outputVar();
        const auto& rowIndex = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(rowIndex, Value(0));

        rewriteMatchClause(
            qctx, static_cast<MatchClauseContext*>(leftCtx), left, right.root, rowIndex);

        auto* cartesianProduct = cartesianProductSegments(qctx, left.root, right.root);
        addInput(left.tail, right.root);
        left.root = cartesianProduct;
        left.tail = right.tail;
        return left;
    } else {
        VLOG(1) << "left tail: " << left.tail->outputVar()
                << "right root: " << right.root->outputVar();
        addInput(left.tail, right.root);
        left.tail = right.tail;
        return left;
    }

    return Status::Error("Can not solve the connect strategy of the two subplan..");
}

PlanNode* SegmentsConnector::innerJoinSegments(QueryContext* qctx,
                                               const PlanNode* left,
                                               const PlanNode* right,
                                               InnerJoinStrategy::JoinPos leftPos,
                                               InnerJoinStrategy::JoinPos rightPos) {
    return std::make_unique<InnerJoinStrategy>(qctx)
                ->leftPos(leftPos)
                ->rightPos(rightPos)
                ->connect(left, right);
}

PlanNode* SegmentsConnector::cartesianProductSegments(QueryContext* qctx,
                                                      const PlanNode* left,
                                                      const PlanNode* right) {
    return std::make_unique<CartesianProductStrategy>(qctx)->connect(left, right);
}

PlanNode* SegmentsConnector::applySegments(QueryContext* qctx,
                                           const PlanNode* left,
                                           const PlanNode* right,
                                           const std::string &rowIndex) {
    return std::make_unique<ApplyStrategy>(qctx, rowIndex)->connect(left, right);
}

void SegmentsConnector::addDependency(const PlanNode* left, const PlanNode* right) {
    std::make_unique<AddDependencyStrategy>()->connect(left, right);
}

void SegmentsConnector::addInput(const PlanNode* left, const PlanNode* right, bool copyColNames) {
    std::make_unique<AddInputStrategy>(copyColNames)->connect(left, right);
}

// iterateDataSet is used to make each row of a dataset be an individual dataset
PlanNode* SegmentsConnector::iterateDataSet(QueryContext* qctx,
                                            PlanNode* input,
                                            const std::string& rowIndex) {
    auto* cols = qctx->objPool()->add(new YieldColumns());
    auto makeColumn = [input, &rowIndex](int i) {
        DCHECK_LT(i, input->colNames().size());
        return new YieldColumn(
            new SubscriptExpression(
                new SubscriptExpression(new VariableExpression(new std::string(input->outputVar())),
                                        new VariableExpression(new std::string(rowIndex))),
                new ConstantExpression(i)),
            new std::string(input->colNames()[i]));
    };
    for (size_t i = 0; i < input->colNames().size(); ++i) {
        cols->addColumn(makeColumn(i));
    }
    auto* project = Project::make(qctx, nullptr, cols);
    project->setColNames(input->colNames());

    return project;
}

PlanNode* SegmentsConnector::transformDataSet(QueryContext* qctx, PlanNode* input) {
    const std::vector<std::string>& colNames = input->colNames();
    auto* cols = qctx->objPool()->add(new YieldColumns());
    auto makeColumn = [](const std::string& colName) {
        return new YieldColumn(
            ExpressionUtils::newVarPropExpr(colName));
    };
    for (auto &colName : colNames) {
        cols->addColumn(makeColumn(colName));
    }
    auto* project = Project::make(qctx, input, cols);
    project->setColNames(colNames);

    return project;
}

Status SegmentsConnector::rewriteMatchClause(QueryContext* qctx,
                                             MatchClauseContext* mctx,
                                             SubPlan& plan,
                                             PlanNode* input,
                                             const std::string& rowIndex) {
    UNUSED(mctx);
    RewriteMatchLabelVisitor visitor([input, rowIndex](const Expression* expr) -> Expression* {
        DCHECK(expr->kind() == Expression::Kind::kLabelAttribute ||
               expr->kind() == Expression::Kind::kLabel);
        if (expr->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<const LabelAttributeExpression*>(expr);
            auto* args = new ArgumentList();
            args->addArgument(
                std::make_unique<VariableExpression>(new std::string(input->outputVar())));
            args->addArgument(std::make_unique<VariableExpression>(new std::string(rowIndex)));
            args->addArgument(la->left()->clone());
            return new AttributeExpression(
                new FunctionCallExpression(new std::string("dataSetRowCol"), args),
                la->right()->clone().release());
        } else {
            auto lb = static_cast<const LabelExpression*>(expr);
            auto* args = new ArgumentList();
            args->addArgument(
                std::make_unique<VariableExpression>(new std::string(input->outputVar())));
            args->addArgument(std::make_unique<VariableExpression>(new std::string(rowIndex)));
            args->addArgument(std::make_unique<ConstantExpression>(*lb->name()));
            return new FunctionCallExpression(new std::string("dataSetRowCol"), args);
        }
    });

    std::vector<PlanNode*> filterNodes;
    collectPlanNodes(plan.root, plan.tail, PlanNode::Kind::kFilter, filterNodes);
    for (auto node : filterNodes) {
        DCHECK_EQ(node->kind(), PlanNode::Kind::kFilter);
        auto filterNode = static_cast<Filter*>(node);
        auto filter = qctx->objPool()->add(filterNode->condition()->clone().release());
        filter->accept(&visitor);
        filterNode->setCondition(filter);
    }

    return Status::OK();
}

void SegmentsConnector::collectPlanNodes(const PlanNode* root,
                                         const PlanNode* tail,
                                         PlanNode::Kind kind,
                                         std::vector<PlanNode*>& result) {
    if (root->kind() == kind) {
        result.emplace_back(const_cast<PlanNode*>(root));
    }
    if (root == tail) {
        return;
    }

    switch (root->dependencies().size()) {
        case 0: {
            // Do nothing
            break;
        }
        case 1: {
            if (root->kind() == PlanNode::Kind::kSelect) {
                auto select = static_cast<const Select *>(root);
                collectPlanNodes(select->then(), tail, kind, result);
                collectPlanNodes(select->otherwise(), tail, kind, result);
            } else if (root->kind() == PlanNode::Kind::kLoop) {
                auto loop = static_cast<const Loop *>(root);
                collectPlanNodes(loop->body(), tail, kind, result);
            }
            auto dep = DCHECK_NOTNULL(root->dep(0));
            collectPlanNodes(dep, tail, kind, result);
            break;
        }
        case 2: {
            auto leftNode = DCHECK_NOTNULL(root->dep(0));
            collectPlanNodes(leftNode, tail, kind, result);
            auto rightNode = DCHECK_NOTNULL(root->dep(1));
            collectPlanNodes(rightNode, tail, kind, result);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid number of plan node dependencies: "
                       << root->dependencies().size();
            break;
        }
    }
}

}   // namespace graph
}   // namespace nebula
