/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LimitExecutor.h"

#include "context/QueryContext.h"
#include "exec/query/test/ExecutorTestBase.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class LimitExecutorTest : public ExecutorTestBase {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        plan_ = qctx_->plan();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ExecutionPlan* plan_;
};

TEST_F(LimitExecutorTest, TestLimitSuccess) {
    std::vector<std::string> colNames = {"col1", "col2"};
    DataSet ids;
    ids.colNames = colNames;
    ids.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto input = StartNode::make(plan_);
    auto limitNode = Limit::make(plan_, input, 1, 2);
    limitNode->setInputVar(input->varName());

    auto limit = Executor::makeExecutor(limitNode, qctx_.get());
    // Must save the values after constructing executors
    auto res = ExecResult::buildSequential(Value(ids), State(State::Stat::kSuccess));
    qctx_->ectx()->setResult(input->varName(), std::move(res));
    auto future = limit->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto& result = qctx_->ectx()->getResult(limitNode->varName());
    EXPECT_TRUE(result.value().isDataSet());

    auto iter = result.iter();
    auto resDS = iterateDataSet(colNames, iter.get());
    EXPECT_TRUE(diffDataSet(resDS, expected));
}

TEST_F(LimitExecutorTest, TestGreaterOffset) {
    std::vector<std::string> colNames = {"col1", "col2"};
    DataSet ids;
    ids.colNames = colNames;
    ids.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto input = StartNode::make(plan_);
    auto limitNode = Limit::make(plan_, input, 4, 2);
    limitNode->setInputVar(input->varName());

    auto limit = Executor::makeExecutor(limitNode, qctx_.get());
    // Must save the values after constructing executors
    auto res = ExecResult::buildSequential(Value(ids), State(State::Stat::kSuccess));
    qctx_->ectx()->setResult(input->varName(), std::move(res));
    auto future = limit->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {};

    auto& result = qctx_->ectx()->getResult(limitNode->varName());
    EXPECT_TRUE(result.value().isDataSet());

    auto iter = result.iter();
    auto resDS = iterateDataSet(colNames, iter.get());
    EXPECT_TRUE(diffDataSet(resDS, expected));
}

TEST_F(LimitExecutorTest, TestGreaterCount) {
    std::vector<std::string> colNames = {"col1", "col2"};
    DataSet ids;
    ids.colNames = colNames;
    ids.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto input = StartNode::make(plan_);
    auto limitNode = Limit::make(plan_, input, 1, 4);
    limitNode->setInputVar(input->varName());

    auto limit = Executor::makeExecutor(limitNode, qctx_.get());
    // Must save the values after constructing executors
    auto res = ExecResult::buildSequential(Value(ids), State(State::Stat::kSuccess));
    qctx_->ectx()->setResult(input->varName(), std::move(res));
    auto future = limit->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto& result = qctx_->ectx()->getResult(limitNode->varName());
    EXPECT_TRUE(result.value().isDataSet());

    auto iter = result.iter();
    auto resDS = iterateDataSet(colNames, iter.get());
    EXPECT_TRUE(diffDataSet(resDS, expected));
}

}   // namespace graph
}   // namespace nebula
