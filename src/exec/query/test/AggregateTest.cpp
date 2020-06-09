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
        // ======================
        // | col1 | col2 | col3 |
        // ----------------------
        // |  0   |  0   |  0   |
        // ----------------------
        // |  1   |  0   |  0   |
        // ----------------------
        // |  2   |  1   |  0   |
        // ----------------------
        // |  3   |  1   |  0   |
        // ----------------------
        // |  4   |  2   |  1   |
        // ----------------------
        // |  5   |  2   |  1   |
        // ----------------------
        // |  6   |  3   |  1   |
        // ----------------------
        // |  7   |  3   |  1   |
        // ----------------------
        // |  8   |  4   |  2   |
        // ----------------------
        // |  9   |  4   |  2   |
        // ----------------------
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
        // ========
        // | col2 |
        // --------
        // |  0   |
        // --------
        // |  1   |
        // --------
        // |  2   |
        // --------
        // |  3   |
        // --------
        // |  4   |
        // --------
        DataSet expected;
        expected.colNames = {"col2"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = col2
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
    {
        // ===============
        // | col2 | col3 |
        // ---------------
        // |  0   |  0   |
        // ---------------
        // |  1   |  0   |
        // ---------------
        // |  2   |  1   |
        // ---------------
        // |  3   |  1   |
        // ---------------
        // |  4   |  2   |
        // ---------------
        DataSet expected;
        expected.colNames = {"col2", "col3"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, col3
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), ""));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "col3"});

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
        // ====================================
        // | list                             |
        // ------------------------------------
        // | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]   |
        // ------------------------------------
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        for (auto i = 0; i < 10; ++i) {
            list.values.emplace_back(i);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(col1)
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
        // ========
        // | list |
        // --------
        // | [ 0 ]|
        // --------
        // | [ 1 ]|
        // --------
        // | [ 2 ]|
        // --------
        // | [ 3 ]|
        // --------
        // | [ 4 ]|
        // --------
        // | [ 5 ]|
        // --------
        // | [ 6 ]|
        // --------
        // | [ 7 ]|
        // --------
        // | [ 8 ]|
        // --------
        // | [ 9 ]|
        // --------
        DataSet expected;
        expected.colNames = {"list"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            List list;
            list.values.emplace_back(i);
            row.columns.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }

        // key = col1
        // items = collect(col1)
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
    {
        // ===================
        // | col2 | list     |
        // -------------------
        // |  0   | [ 0, 0 ] |
        // -------------------
        // |  1   | [ 0, 0 ] |
        // -------------------
        // |  2   | [ 1, 1 ] |
        // -------------------
        // |  3   | [ 1, 1 ] |
        // -------------------
        // |  4   | [ 2, 2 ] |
        // -------------------
        DataSet expected;
        expected.colNames = {"list"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            List list;
            list.values = {i / 2, i / 2};
            row.columns.emplace_back(i);
            row.columns.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = col2, collect(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupItems.emplace_back(std::make_pair(expr1.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "col3"});

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
        // ========
        // | count|
        // --------
        // | 10   |
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(col1)
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
        // ========
        // | count|
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        DataSet expected;
        expected.colNames = {"count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count(col2)
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
    {
        // ================
        // | col3 | count |
        // ----------------
        // |  0   |   2   |
        // ----------------
        // |  0   |   2   |
        // ----------------
        // |  1   |   2   |
        // ----------------
        // |  1   |   2   |
        // ----------------
        // |  2   |   2   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col3", "count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i / 2);
            row.columns.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col3, col2
        // items = col3, count(col2)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), ""));
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCount));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col3", "count"});

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
        // ========
        // | sum  |
        // --------
        // | 45   |
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(45);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(col1)
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
        // ========
        // | sum  |
        // --------
        // |   0  |
        // --------
        // |   4  |
        // --------
        // |   8  |
        // --------
        // |   12 |
        // --------
        // |   16 |
        DataSet expected;
        expected.colNames = {"sum"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2 * i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = sum(col2)
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
    {
        // ================
        // | col2 | sum   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   2   |
        // ----------------
        // |  3   |   2   |
        // ----------------
        // |  4   |   4   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "sum"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back((i / 2) * 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, sum(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kSum));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "sum"});

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
        // ========
        // | avg  |
        // --------
        // | 4.5  |
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(4.5);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(col1)
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
        // ========
        // | avg  |
        // --------
        // |   0  |
        // --------
        // |   1  |
        // --------
        // |   2  |
        // --------
        // |   3  |
        // --------
        // |   4  |
        DataSet expected;
        expected.colNames = {"avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = avg(col2)
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
    {
        // ================
        // | col2 | avg   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, sum(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kAvg));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "avg"});

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
        // ===============
        // | count_dist  |
        // ---------------
        // |    10       |
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count_dist(col1)
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
        // ===============
        // | count_dist  |
        // ---------------
        // |   1         |
        // ---------------
        // |   1         |
        // ---------------
        // |   1         |
        // ---------------
        // |   1         |
        // ---------------
        // |   1         |
        DataSet expected;
        expected.colNames = {"count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count_dist(col2)
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
    {
        // =======================
        // | col2 | count_dist   |
        // -----------------------
        // |  0   |   1          |
        // -----------------------
        // |  1   |   1          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   1          |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, count_dist(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kCountDist));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "count"});

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
        // ========
        // | max  |
        // --------
        // | 10   |
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(9);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(col1)
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
    {
        // ================
        // | col2 | max   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "max"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, max(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kMax));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "max"});

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

TEST_F(AggregateTest, Min) {
    {
        // ========
        // | min  |
        // --------
        // | 10   |
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(col1)
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
    {
        // ================
        // | col2 | min   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "min"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, min(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kMin));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "min"});

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

TEST_F(AggregateTest, Stdev) {
    {
        // ===============
        // | stdev       |
        // ---------------
        // |2.87228132327|
        DataSet expected;
        expected.colNames = {"stdev"};
        Row row;
        row.emplace_back(2.87228132327);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(col1)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kStd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"stdev"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        // ===============
        // | stdev      |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        DataSet expected;
        expected.colNames = {"stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = stdev(col2)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kStd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"stdev"});

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
    {
        // =======================
        // | col2 | stdev       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   0          |
        // -----------------------
        // |  3   |   0          |
        // -----------------------
        // |  4   |   0          |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, stdev(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kStd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "stdev"});

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

TEST_F(AggregateTest, BitAnd) {
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     0       |
        DataSet expected;
        expected.colNames = {"bit_and"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(col1)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kBitAnd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_and"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        DataSet expected;
        expected.colNames = {"bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_and(col2)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kBitAnd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_and"});

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
    {
        // =======================
        // | col2 | bit_and      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_and(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kBitAnd));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "bit_and"});

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

TEST_F(AggregateTest, BitOr) {
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    15       |
        DataSet expected;
        expected.colNames = {"bit_or"};
        Row row;
        row.emplace_back(15);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(col1)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kBitOr));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_or"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        DataSet expected;
        expected.colNames = {"bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_or(col2)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kBitOr));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_or"});

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
    {
        // =======================
        // | col2 | bit_or       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_or(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kBitOr));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "bit_or"});

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

TEST_F(AggregateTest, BitXor) {
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        DataSet expected;
        expected.colNames = {"bit_xor"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(col1)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupItems.emplace_back(std::make_pair(expr.get(), kBitXor));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_xor"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        DataSet expected;
        expected.colNames = {"bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_xor(col2)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kBitXor));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"bit_xor"});

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
    {
        // =======================
        // | col2 | bit_xor      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   0          |
        // -----------------------
        // |  3   |   0          |
        // -----------------------
        // |  4   |   0          |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_xor(col3)
        std::vector<Expression*> groupKeys;
        std::vector<Aggregate::GroupItem> groupItems;
        auto expr = std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 = std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupKeys.emplace_back(expr1.get());
        groupItems.emplace_back(std::make_pair(expr1.get(), kBitXor));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "bit_xor"});

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
}  // namespace graph
}  // namespace nebula
