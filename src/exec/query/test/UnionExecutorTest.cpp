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

    static bool diffDataSet(const DataSet& lhs, const DataSet& rhs) {
        if (lhs.colNames != rhs.colNames) return false;
        if (lhs.rows.size() != rhs.rows.size()) return false;
        return diffDataSetInner(lhs, rhs) && diffDataSetInner(rhs, lhs);
    }

    static bool diffDataSetInner(const DataSet& lhs, const DataSet& rhs) {
        for (auto& lrow : lhs.rows) {
            bool found = false;
            for (auto& rrow : rhs.rows) {
                if (lrow == rrow) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ExecutionPlan* plan_;
};

TEST_F(UnionExecutorTest, TestBase) {
    DataSet lds;
    lds.colNames = {"lcol1", "lcol2"};
    lds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
    };

    DataSet rds;
    rds.colNames = {"rcol1", "rcol2"};
    rds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(3), Value("row3")}),
    };

    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());
    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->varName(), Value(lds));
    qctx_->ectx()->setValue(right->varName(), Value(rds));
    auto future = unionExecutor->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    DataSet expected;
    expected.colNames = {"lcol1", "lcol2"};
    expected.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto result = qctx_->ectx()->getValue(unionNode->varName());
    EXPECT_EQ(result.type(), Value::Type::DATASET);

    auto resultDS = result.moveDataSet();
    EXPECT_TRUE(diffDataSet(resultDS, expected));
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

TEST_F(UnionExecutorTest, TestUionAll) {
    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right, false);

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());

    DataSet lds;
    lds.colNames = {"lcol1", "lcol2"};
    lds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
    };

    DataSet rds;
    rds.colNames = {"rcol1", "rcol2"};
    rds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(3), Value("row3")}),
    };

    DataSet expected;
    expected.colNames = {"lcol1", "lcol2"};
    expected.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    qctx_->ectx()->setValue(left->varName(), Value(lds));
    qctx_->ectx()->setValue(right->varName(), Value(rds));

    auto future = unionExecutor->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto result = qctx_->ectx()->getValue(unionNode->varName());
    EXPECT_TRUE(diffDataSet(result.getDataSet(), expected));
}

}   // namespace graph
}   // namespace nebula
