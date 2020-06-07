/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/AggregateExecutor.h"

namespace nebula {
namespace graph {
class AggregateTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        input_ = std::make_unique<std::string>("input_agg");
        qctx_ = std::make_unique<QueryContext>();
        DataSet ds;
        ds.colNames = {"col1", "col2", "col3"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            row.columns.emplace_back(i);
            int64_t col2 = i / 2;
            row.columns.emplace_back(col2);
            int64_t col3 = i / 4;
            row.columns.emplace_back(col3);
            ds.rows.emplace_back(std::move(row));
        }

        qctx_->ectx()->setResult(*input_,
                    ExecResult::buildSequential(Value(ds), State()));
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
    static std::unique_ptr<std::string> input_;
};

std::unique_ptr<QueryContext> AggregateTest::qctx_;
std::unique_ptr<std::string> AggregateTest::input_;

struct RowCmp {
    bool operator() (const Row& lhs, const Row& rhs) {
        DCHECK_EQ(lhs.columns.size(), rhs.columns.size());
        for (size_t i = 0; i < lhs.columns.size(); ++i) {
            if (lhs.columns[i] < rhs.columns[i]) {
                return true;
            }
        }
        return false;
    }
};

TEST_F(AggregateTest, Group) {
    {
        DataSet expected;
        expected.colNames = {"col2"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Collect) {
    {
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        for (auto i = 0; i < 10; ++i) {
            list.values.emplace_back(i);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        DataSet expected;
        expected.colNames = {"list"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            List list;
            list.values.emplace_back(i);
            row.columns.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        // TODO: Implement the < for list.
        // EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Count) {
    {
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kCount));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"count"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        DataSet expected;
        expected.colNames = {"count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCount));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"count"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Sum) {
    {
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(45);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kSum));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"sum"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        DataSet expected;
        expected.colNames = {"sum"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2 * i);
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kSum));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"sum"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Avg) {
    {
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(4.5);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kAvg));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"avg"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        DataSet expected;
        expected.colNames = {"avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kAvg));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"avg"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, CountDistinct) {
    {
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kCountDist));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"count"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        DataSet expected;
        expected.colNames = {"count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCountDist));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"count"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Max) {
    {
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(9);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kMax));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"max"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}

TEST_F(AggregateTest, Min) {
    {
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kMin));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"min"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}
}  // namespace graph
}  // namespace nebula
