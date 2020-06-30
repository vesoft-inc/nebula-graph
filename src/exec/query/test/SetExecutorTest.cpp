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

class SetExecutorTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        plan_ = qctx_->plan();
    }

    static bool diffDataSet(const DataSet& lhs, const DataSet& rhs) {
        if (lhs.colNames != rhs.colNames) return false;
        if (lhs.rows.size() != rhs.rows.size()) return false;

        auto comp = [](const Row& l, const Row& r) -> bool {
            for (size_t i = 0; i < l.values.size(); ++i) {
                if (!(l.values[i] < r.values[i])) return false;
            }
            return true;
        };

        // Following sort will change the input data sets, so make the copies
        auto l = lhs;
        auto r = rhs;
        std::sort(l.rows.begin(), l.rows.end(), comp);
        std::sort(r.rows.begin(), r.rows.end(), comp);
        return l.rows == r.rows;
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ExecutionPlan* plan_;
};

TEST_F(SetExecutorTest, TestUnionAll) {
    std::vector<std::string> colNames = {"col1", "col2"};
    DataSet lds;
    lds.colNames = colNames;
    lds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
    };

    DataSet rds;
    rds.colNames = colNames;
    rds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(3), Value("row3")}),
    };

    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);
    unionNode->setLeftVar(left->varName());
    unionNode->setRightVar(right->varName());

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());
    // Must save the values after constructing executors
    qctx_->ectx()->setResult(left->varName(), ExecResult::buildSequential(Value(lds), State()));
    qctx_->ectx()->setResult(right->varName(), ExecResult::buildSequential(Value(rds), State()));
    auto future = unionExecutor->execute();
    EXPECT_TRUE(std::move(future).get().ok());

    DataSet expected;
    expected.colNames = colNames;
    expected.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
        Row({Value(3), Value("row3")}),
    };

    auto& result = qctx_->ectx()->getResult(unionNode->varName());
    EXPECT_TRUE(result.value().isDataSet());

    DataSet resultDS;
    resultDS.colNames = result.value().getDataSet().colNames;
    for (auto iter = result.iter(); iter->valid(); iter->next()) {
        Row row;
        for (auto& col : resultDS.colNames) {
            row.values.emplace_back(iter->getColumn(col));
        }
        resultDS.emplace_back(std::move(row));
    }

    EXPECT_TRUE(diffDataSet(resultDS, expected));
}

TEST_F(SetExecutorTest, TestUnionDifferentColumns) {
    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);
    unionNode->setLeftVar(left->varName());
    unionNode->setRightVar(right->varName());

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

    EXPECT_FALSE(status.ok());

    auto expected = "Data sets have different columns: <col1> vs. <col1,col2>";
    EXPECT_EQ(status.toString(), expected);
}

TEST_F(SetExecutorTest, TestUnionDifferentValueType) {
    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto unionNode = Union::make(plan_, left, right);
    unionNode->setLeftVar(left->varName());
    unionNode->setRightVar(right->varName());

    auto unionExecutor = Executor::makeExecutor(unionNode, qctx_.get());

    List lst;
    DataSet rds;

    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->varName(), Value(lst));
    qctx_->ectx()->setValue(right->varName(), Value(rds));
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_FALSE(status.ok());

    std::stringstream ss;
    ss << "Invalid data types of dependencies: " << Value::Type::LIST << " vs. "
       << Value::Type::DATASET << ".";
    EXPECT_EQ(status.toString(), ss.str());
}

TEST_F(SetExecutorTest, TestIntersect) {
    DataSet lds;
    lds.colNames = {"col1", "col2"};
    lds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
    };

    DataSet rds;
    rds.colNames = {"col1", "col2"};
    rds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(3), Value("row3")}),
    };

    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto intersect = Intersect::make(plan_, left, right);
    intersect->setLeftVar(left->varName());
    intersect->setRightVar(right->varName());

    auto executor = Executor::makeExecutor(intersect, qctx_.get());
    qctx_->ectx()->setResult(left->varName(), ExecResult::buildSequential(Value(lds), State()));
    qctx_->ectx()->setResult(right->varName(), ExecResult::buildSequential(Value(rds), State()));

    DataSet expected;
    expected.colNames = {"col1", "col2"};
    expected.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(1), Value("row1")}),
    };

    auto fut = executor->execute();
    auto status = std::move(fut).get();
    EXPECT_TRUE(status.ok());

    auto& result = qctx_->ectx()->getResult(intersect->varName());
    EXPECT_TRUE(result.value().isDataSet());

    DataSet ds;
    ds.colNames = lds.colNames;
    for (auto iter = result.iter(); iter->valid(); iter->next()) {
        Row row;
        for (auto& col : ds.colNames) {
            row.values.emplace_back(iter->getColumn(col));
        }
        ds.emplace_back(std::move(row));
    }
    EXPECT_TRUE(diffDataSet(ds, expected));
}

TEST_F(SetExecutorTest, TestMinus) {
    DataSet lds;
    lds.colNames = {"col1", "col2"};
    lds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(1), Value("row1")}),
        Row({Value(2), Value("row2")}),
    };

    DataSet rds;
    rds.colNames = {"col1", "col2"};
    rds.rows = {
        Row({Value(1), Value("row1")}),
        Row({Value(3), Value("row3")}),
    };

    auto left = StartNode::make(plan_);
    auto right = StartNode::make(plan_);
    auto minus = Minus::make(plan_, left, right);
    minus->setLeftVar(left->varName());
    minus->setRightVar(right->varName());

    auto executor = Executor::makeExecutor(minus, qctx_.get());
    qctx_->ectx()->setResult(left->varName(), ExecResult::buildSequential(Value(lds), State()));
    qctx_->ectx()->setResult(right->varName(), ExecResult::buildSequential(Value(rds), State()));

    DataSet expected;
    expected.colNames = {"col1", "col2"};
    expected.rows = {
        Row({Value(2), Value("row2")}),
    };

    auto fut = executor->execute();
    auto status = std::move(fut).get();
    EXPECT_TRUE(status.ok());

    auto& result = qctx_->ectx()->getResult(minus->varName());
    EXPECT_TRUE(result.value().isDataSet());

    DataSet ds;
    ds.colNames = lds.colNames;
    for (auto iter = result.iter(); iter->valid(); iter->next()) {
        Row row;
        for (auto& col : ds.colNames) {
            row.values.emplace_back(iter->getColumn(col));
        }
        ds.emplace_back(std::move(row));
    }
    EXPECT_TRUE(diffDataSet(ds, expected));
}

}   // namespace graph
}   // namespace nebula
