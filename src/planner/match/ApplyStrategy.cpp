/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/ApplyStrategy.h"

#include "common/expression/VariableExpression.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
PlanNode *ApplyStrategy::connect(const PlanNode *left, const PlanNode *right) {
    auto *args = new ArgumentList();
    args->addArgument(std::make_unique<VariableExpression>(new std::string(right->outputVar())));
    auto *condition = new RelationalExpression(
        Expression::Kind::kRelLT,
        new UnaryExpression(Expression::Kind::kUnaryIncr,
                            new VariableExpression(new std::string(rowIndex_))),
        new FunctionCallExpression(new std::string("size"), args));
    qctx_->objPool()->add(condition);
    auto *loop =
        Loop::make(qctx_, const_cast<PlanNode *>(right), const_cast<PlanNode *>(left), condition);
    auto *collect =
        DataCollect::make(qctx_, loop, DataCollect::CollectKind::kAppend, {left->outputVar()});
    collect->setColNames(left->colNames());
    return collect;
}
}   // namespace graph
}   // namespace nebula
