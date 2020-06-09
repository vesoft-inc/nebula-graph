/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/UnionExecutor.h"

#include <memory>

#include <folly/String.h>
#include <gtest/gtest.h>

#include "context/ExecutionContext.h"
#include "context/QueryContext.h"
#include "planner/Query.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

class UnionExecutorTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        plan_ = qctx_->plan();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ExecutionPlan* plan_;
};

TEST_F(UnionExecutorTest, TestBase) {
    std::vector<std::string> colNames = {"col1", "col2"};
    DataSet ds[2];
    ds[0].colNames = colNames;
    ds[0].rows.resize(3);
    ds[1].colNames = colNames;
    ds[1].rows.resize(5);
    for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < ds[i].rowSize(); ++j) {
            Row row;
            row.columns = {Value(stringPrintf("ds%lur%lu", i, j)), Value(static_cast<int64_t>(j))};
            ds[i].rows[j] = std::move(row);
        }
    }

    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());
    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->varName(), Value(ds[0]));
    qctx_->ectx()->setValue(right->varName(), Value(ds[1]));
    auto future = unionExecutor->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    auto result = qctx_->ectx()->getValue(unionNode->varName());
    EXPECT_EQ(result.type(), Value::Type::DATASET);
    auto resultDS = result.moveDataSet();
    EXPECT_EQ(resultDS.colNames, colNames);
    EXPECT_EQ(resultDS.rowSize(), ds[0].rowSize() + ds[1].rowSize());

    for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < ds[i].rowSize(); ++j) {
            auto col1 = stringPrintf("ds%lur%lu", i, j);
            bool found = false;
            for (size_t k = 0; k < resultDS.rowSize(); ++k) {
                auto& columns = resultDS.rows[k].columns;
                EXPECT_EQ(columns.size(), 2);
                if (columns[0] == col1 && columns[1] == static_cast<int64_t>(j)) {
                    found = true;
                    break;
                }
            }
            EXPECT_TRUE(found);
        }
    }
}

TEST_F(UnionExecutorTest, TestDifferentColumns) {
    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());

    DataSet lds;
    lds.colNames = {"col1"};
    DataSet rds;
    rds.colNames = {"col1", "col2"};

    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->varName(), Value(lds));
    qctx_->ectx()->setValue(right->varName(), Value(rds));
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_EQ(status.toString(),
              "The data sets to union have different columns: <col1> vs. <col1,col2>");
}

TEST_F(UnionExecutorTest, TestDifferentValueType) {
    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());

    List lst;
    DataSet rds;

    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->varName(), Value(lst));
    qctx_->ectx()->setValue(right->varName(), Value(rds));
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_EQ(status.toString(),
              folly::stringPrintf("Invalid data types of dependencies: %d vs. %d.",
                                  static_cast<uint8_t>(Value::Type::LIST),
                                  static_cast<uint8_t>(Value::Type::DATASET)));
}
}   // namespace graph
}   // namespace nebula
