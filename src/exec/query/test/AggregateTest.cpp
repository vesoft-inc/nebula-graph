/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "exec/query/AggregateExecutor.h"
#include "planner/Query.h"

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

        qctx_->ectx()->setResult(
            *input_, ExecResult::buildSequential(Value(ds), State()));
    }

   protected:
    static std::unique_ptr<QueryContext> qctx_;
    static std::unique_ptr<std::string> input_;
};

std::unique_ptr<QueryContext> AggregateTest::qctx_;
std::unique_ptr<std::string> AggregateTest::input_;

struct RowCmp {
    bool operator()(const Row& lhs, const Row& rhs) {
        DCHECK_EQ(lhs.columns.size(), rhs.columns.size());
        for (size_t i = 0; i < lhs.columns.size(); ++i) {
            if (lhs.columns[i] < rhs.columns[i]) {
                return true;
            }
        }
        return false;
    }
};

#define TEST_AGG_1(FUN)                                                     \
    std::vector<Expression*> groupKeys;                                     \
    std::vector<Aggregate::GroupItem> groupItems;                           \
    auto expr =                                                             \
        std::make_unique<InputPropertyExpression>(new std::string("col1")); \
    groupItems.emplace_back(std::make_pair(expr.get(), FUN));               \
    auto* plan = qctx_->plan();                                             \
    auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys),        \
                                std::move(groupItems));                     \
    agg->setInputVar(*input_);                                              \
    agg->setColNames(std::vector<std::string>{FUN});                        \
                                                                            \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());    \
    auto future = aggExe->execute();                                        \
    auto status = std::move(future).get();                                  \
    EXPECT_TRUE(status.ok());                                               \
    auto& result = qctx_->ectx()->getResult(agg->varName());                \
    EXPECT_EQ(result.value().getDataSet(), expected);                       \
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);

#define TEST_AGG_2(FUN)                                                     \
    std::vector<Expression*> groupKeys;                                     \
    std::vector<Aggregate::GroupItem> groupItems;                           \
    auto expr =                                                             \
        std::make_unique<InputPropertyExpression>(new std::string("col2")); \
    groupKeys.emplace_back(expr.get());                                     \
    groupItems.emplace_back(std::make_pair(expr.get(), FUN));               \
    auto* plan = qctx_->plan();                                             \
    auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys),        \
                                std::move(groupItems));                     \
    agg->setInputVar(*input_);                                              \
    agg->setColNames(std::vector<std::string>{FUN});                        \
                                                                            \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());    \
    auto future = aggExe->execute();                                        \
    auto status = std::move(future).get();                                  \
    EXPECT_TRUE(status.ok());                                               \
    auto& result = qctx_->ectx()->getResult(agg->varName());                \
    DataSet sortedDs = result.value().getDataSet();                         \
    std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());        \
    EXPECT_EQ(sortedDs, expected);                                          \
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);

#define TEST_AGG_3(FUN)                                                     \
    std::vector<Expression*> groupKeys;                                     \
    std::vector<Aggregate::GroupItem> groupItems;                           \
    auto expr =                                                             \
        std::make_unique<InputPropertyExpression>(new std::string("col2")); \
    groupKeys.emplace_back(expr.get());                                     \
    groupItems.emplace_back(std::make_pair(expr.get(), ""));                \
    auto expr1 =                                                            \
        std::make_unique<InputPropertyExpression>(new std::string("col3")); \
    groupKeys.emplace_back(expr1.get());                                    \
    groupItems.emplace_back(std::make_pair(expr1.get(), FUN));              \
    auto* plan = qctx_->plan();                                             \
    auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys),        \
                                std::move(groupItems));                     \
    agg->setInputVar(*input_);                                              \
    agg->setColNames(std::vector<std::string>{"col2", FUN});                \
                                                                            \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());    \
    auto future = aggExe->execute();                                        \
    auto status = std::move(future).get();                                  \
    EXPECT_TRUE(status.ok());                                               \
    auto& result = qctx_->ectx()->getResult(agg->varName());                \
    DataSet sortedDs = result.value().getDataSet();                         \
    std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());        \
    EXPECT_EQ(sortedDs, expected);                                          \
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);

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
        expected.colNames = {""};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = col2
        TEST_AGG_2("")
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
        expected.colNames = {"col2", ""};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, col3
        TEST_AGG_3("")
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
        expected.colNames = {kCollect};
        Row row;
        List list;
        for (auto i = 0; i < 10; ++i) {
            list.values.emplace_back(i);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(col1)
        TEST_AGG_1(kCollect)
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
        auto expr =
            std::make_unique<InputPropertyExpression>(new std::string("col1"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys),
                                    std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        auto& ds = result.value().getDataSet();
        std::vector<Value> vals;
        for (auto& r : ds.rows) {
            for (auto& c : r.columns) {
                EXPECT_EQ(c.type(), Value::Type::LIST);
                auto& list = c.getList();
                EXPECT_EQ(list.size(), 1);
                for (auto& v : list.values) {
                    vals.emplace_back(std::move(v));
                }
            }
        }
        std::vector<Value> expectedVals = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::sort(vals.begin(), vals.end());
        EXPECT_EQ(vals, expectedVals);
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
        auto expr =
            std::make_unique<InputPropertyExpression>(new std::string("col2"));
        groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), ""));
        auto expr1 =
            std::make_unique<InputPropertyExpression>(new std::string("col3"));
        groupItems.emplace_back(std::make_pair(expr1.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys),
                                    std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"col2", "col3"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        auto& ds = result.value().getDataSet();
        std::vector<Value> vals;
        for (auto& r : ds.rows) {
            EXPECT_EQ(r.columns.size(), 2);
            auto& c = r.columns[1];
            EXPECT_EQ(c.type(), Value::Type::LIST);
            auto& list = c.getList();
            EXPECT_EQ(list.size(), 2);
            for (auto& v : list.values) {
                vals.emplace_back(std::move(v));
            }
        }
        std::vector<Value> expectedVals = {0, 0, 0, 0, 1, 1, 1, 1, 2, 2};
        std::sort(vals.begin(), vals.end());
        EXPECT_EQ(vals, expectedVals);
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
        expected.colNames = {kCount};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(col1)
        TEST_AGG_1(kCount)
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
        expected.colNames = {kCount};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count(col2)
        TEST_AGG_2(kCount)
    }
    {
        // ================
        // | col3 | count |
        // ----------------
        // |  0   |   2   |
        // ----------------
        // |  1   |   2   |
        // ----------------
        // |  2   |   2   |
        // ----------------
        // |  3   |   2   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", kCount};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col3, col2
        // items = col3, count(col2)
        TEST_AGG_3(kCount)
    }
}

TEST_F(AggregateTest, Sum) {
    {
        // ========
        // | sum  |
        // --------
        // | 45   |
        DataSet expected;
        expected.colNames = {kSum};
        Row row;
        row.emplace_back(45);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(col1)
        TEST_AGG_1(kSum)
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
        expected.colNames = {kSum};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(2 * i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = sum(col2)
        TEST_AGG_2(kSum)
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
        expected.colNames = {"col2", kSum};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back((i / 2) * 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, sum(col3)
        TEST_AGG_3(kSum)
    }
}

TEST_F(AggregateTest, Avg) {
    {
        // ========
        // | avg  |
        // --------
        // | 4.5  |
        DataSet expected;
        expected.colNames = {kAvg};
        Row row;
        row.emplace_back(4.5);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(col1)
        TEST_AGG_1(kAvg)
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
        expected.colNames = {kAvg};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = avg(col2)
        TEST_AGG_2(kAvg)
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
        expected.colNames = {"col2", kAvg};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, sum(col3)
        TEST_AGG_3(kAvg)
    }
}

TEST_F(AggregateTest, CountDistinct) {
    {
        // ===============
        // | count_dist  |
        // ---------------
        // |    10       |
        DataSet expected;
        expected.colNames = {kCountDist};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count_dist(col1)
        TEST_AGG_1(kCountDist)
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
        expected.colNames = {kCountDist};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count_dist(col2)
        TEST_AGG_2(kCountDist)
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
        expected.colNames = {"col2", kCountDist};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, count_dist(col3)
        TEST_AGG_3(kCountDist)
    }
}

TEST_F(AggregateTest, Max) {
    {
        // ========
        // | max  |
        // --------
        // | 10   |
        DataSet expected;
        expected.colNames = {kMax};
        Row row;
        row.emplace_back(9);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(col1)
        TEST_AGG_1(kMax)
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
        expected.colNames = {"col2", kMax};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, max(col3)
        TEST_AGG_3(kMax)
    }
}

TEST_F(AggregateTest, Min) {
    {
        // ========
        // | min  |
        // --------
        // | 10   |
        DataSet expected;
        expected.colNames = {kMin};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(col1)
        TEST_AGG_1(kMin)
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
        expected.colNames = {"col2", kMin};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, min(col3)
        TEST_AGG_3(kMin)
    }
}

TEST_F(AggregateTest, Stdev) {
    {
        // ===============
        // | stdev       |
        // ---------------
        // |2.87228132327|
        DataSet expected;
        expected.colNames = {kStd};
        Row row;
        row.emplace_back(2.87228132327);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(col1)
        TEST_AGG_1(kStd)
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
        expected.colNames = {kStd};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = stdev(col2)
        TEST_AGG_2(kStd)
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
        expected.colNames = {"col2", kStd};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, stdev(col3)
        TEST_AGG_3(kStd)
    }
}

TEST_F(AggregateTest, BitAnd) {
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     0       |
        DataSet expected;
        expected.colNames = {kBitAnd};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(col1)
        TEST_AGG_1(kBitAnd)
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
        expected.colNames = {kBitAnd};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_and(col2)
        TEST_AGG_2(kBitAnd)
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
        expected.colNames = {"col2", kBitAnd};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_and(col3)
        TEST_AGG_3(kBitAnd)
    }
}

TEST_F(AggregateTest, BitOr) {
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    15       |
        DataSet expected;
        expected.colNames = {kBitOr};
        Row row;
        row.emplace_back(15);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(col1)
        TEST_AGG_1(kBitOr)
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
        expected.colNames = {kBitOr};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_or(col2)
        TEST_AGG_2(kBitOr)
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
        expected.colNames = {"col2", kBitOr};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_or(col3)
        TEST_AGG_3(kBitOr)
    }
}

TEST_F(AggregateTest, BitXor) {
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        DataSet expected;
        expected.colNames = {kBitXor};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(col1)
        TEST_AGG_1(kBitXor)
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
        expected.colNames = {kBitXor};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = bit_xor(col2)
        TEST_AGG_2(kBitXor)
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
        expected.colNames = {"col2", kBitXor};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.columns.emplace_back(i);
            row.columns.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2, col3
        // items = col2, bit_xor(col3)
        TEST_AGG_3(kBitXor)
    }
}
}  // namespace graph
}  // namespace nebula
