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

}   // namespace graph
}   // namespace nebula
