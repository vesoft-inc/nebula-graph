/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/UnwindExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class UnwindTest : public QueryTestBase {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        start_ = StartNode::make(qctx_.get());
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    StartNode* start_;
};

TEST_F(UnwindTest, UnwindList) {
    // UNWIND [1, 2, 3] as r
    auto exprList = new ExpressionList(3);
    for (auto i = 0; i < 3; ++i) {
        exprList->add(new ConstantExpression(i));
    }

    auto *col = new YieldColumn(new ListExpression(exprList), new std::string("r"));
    auto *columns = new YieldColumns();
    qctx_->objPool()->add(columns);
    columns->addColumn(col);

    auto* unwind = Unwind::make(qctx_.get(), start_, columns);
    unwind->setColNames(std::vector<std::string>{"r"});

    auto unwExe = Executor::create(unwind, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(unwind->outputVar());

    DataSet expected;
    expected.colNames = {"r"};
    for (auto i = 0; i < 3; ++i) {
        Row row;
        row.values.emplace_back(i);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

// TEST_F(UnwindTest, UnwindNestedList) {
//     // UNWIND [1, [2, NULL, 3], 4, NULL] as r
//     auto nestedList = std::make_unique<ExpressionList>(3);
//     nestedList->add(new ConstantExpression(2));
//     nestedList->add(new ConstantExpression(Value::kNullValue));
//     nestedList->add(new ConstantExpression(3));
//     auto nestedExpr = std::make_unique<ListExpression>(nestedList.release());

//     auto exprList = std::make_unique<ExpressionList>(4);
//     exprList->add(new ConstantExpression(1));
//     exprList->add(nestedExpr.release());
//     exprList->add(new ConstantExpression(4));
//     exprList->add(new ConstantExpression(Value::kNullValue));
//     auto expr = std::make_unique<ListExpression>(exprList.release());

//     auto* unwind = Unwind::make(qctx_.get(), start_, expr.get());
//     unwind->setColNames(std::vector<std::string>{"r"});

//     auto unwExe = Executor::create(unwind, qctx_.get());
//     auto future = unwExe->execute();
//     auto status = std::move(future).get();
//     EXPECT_TRUE(status.ok());
//     auto& result = qctx_->ectx()->getResult(unwind->outputVar());

//     DataSet expected;
//     expected.colNames = {"r"};
//     expected.rows = {Row({Value(1)}),
//                      Row({Value(List({Value(2), Value::kNullValue, Value(3)}))}),
//                      Row({Value(4)}),
//                      Row({Value::kNullValue})};
//     EXPECT_EQ(result.value().getDataSet(), expected);
//     EXPECT_EQ(result.state(), Result::State::kSuccess);
// }

// TEST_F(UnwindTest, UnwindLabel) {
//     // UNWIND [1, [2, NULL, 3], 4, NULL] as r1 UNWIND r as r2
//     auto nestedList = std::make_unique<ExpressionList>(3);
//     nestedList->add(new ConstantExpression(2));
//     nestedList->add(new ConstantExpression(Value::kNullValue));
//     nestedList->add(new ConstantExpression(3));
//     auto nestedExpr = std::make_unique<ListExpression>(nestedList.release());

//     auto exprList = std::make_unique<ExpressionList>(4);
//     exprList->add(new ConstantExpression(1));
//     exprList->add(nestedExpr.release());
//     exprList->add(new ConstantExpression(4));
//     exprList->add(new ConstantExpression(Value::kNullValue));
//     auto expr = std::make_unique<ListExpression>(exprList.release());

//     auto* unwind1 = Unwind::make(qctx_.get(), start_, expr.get());
//     unwind1->setColNames(std::vector<std::string>{"r1"});
//     auto unwExe = Executor::create(unwind1, qctx_.get());
//     auto future = unwExe->execute();
//     auto status = std::move(future).get();
//     EXPECT_TRUE(status.ok());

//     auto vp =
//         std::make_unique<VariablePropertyExpression>(new std::string(), new std::string("r1"));
//     auto* unwind2 = Unwind::make(qctx_.get(), unwind1, vp.get());
//     unwind2->setInputVar(unwind1->outputVar());
//     unwind2->setColNames(std::vector<std::string>{"r2"});
//     unwExe = Executor::create(unwind2, qctx_.get());
//     future = unwExe->execute();
//     status = std::move(future).get();
//     EXPECT_TRUE(status.ok());
//     auto& result = qctx_->ectx()->getResult(unwind2->outputVar());

//     DataSet expected;
//     expected.colNames = {"r2"};
//     expected.rows = {Row({Value(1)}),
//                      Row({Value(2)}),
//                      Row({Value::kNullValue}),
//                      Row({Value(3)}),
//                      Row({Value(4)})};
//     EXPECT_EQ(result.value().getDataSet(), expected);
//     EXPECT_EQ(result.state(), Result::State::kSuccess);
// }

}   // namespace graph
}   // namespace nebula
