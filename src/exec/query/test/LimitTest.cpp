/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LimitExecutor.h"

#include "exec/query/test/ExecutorTestBase.h"

namespace nebula {
namespace graph {

class LimitExecutorTest : public ExecutorTestBase {
public:
    void test(const DataSet& inputDS, const DataSet& expected, int32_t offset, int32_t count) {
        auto input = StartNode::make(plan_);
        auto limitNode = Limit::make(plan_, input, offset, count);
        limitNode->setInputVar(input->varName());

        auto limit = Executor::makeExecutor(limitNode, qctx_.get());
        // Must save the values after constructing executors
        auto res = ExecResult::buildSequential(Value(inputDS), State(State::Stat::kSuccess));
        qctx_->ectx()->setResult(input->varName(), std::move(res));
        auto future = limit->execute();
        EXPECT_TRUE(std::move(future).get().ok());

        auto& result = qctx_->ectx()->getResult(limitNode->varName());
        EXPECT_TRUE(result.value().isDataSet());

        auto iter = result.iter();
        auto resDS = iterateDataSet(inputDS.colNames, iter.get());
        EXPECT_TRUE(diffDataSet(resDS, expected));
    }
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

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    test(ids, expected, 1, 2);
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

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {};

    test(ids, expected, 4, 2);
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

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    test(ids, expected, 1, 4);
}

}   // namespace graph
}   // namespace nebula
