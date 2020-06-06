/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/ProjectExecutor.h"
#include "exec/query/AggregateExecutor.h"

namespace nebula {
namespace graph {
class QueryExecutorsTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        qctx_ = std::make_unique<QueryContext>();
    }
protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> QueryExecutorsTest::qctx_;

TEST_F(QueryExecutorsTest, Project) {
    std::string input = "input_project";
    DataSet ds;
    ds.colNames.emplace_back("_vid");
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.columns.emplace_back(i);
        ds.rows.emplace_back(std::move(row));
    }
    auto expected = ds;
    qctx_->ectx()->setResult(input,
                ExecResult::buildSequential(Value(std::move(ds)), State()));

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(input),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column);
    auto* plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"_vid"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->varName());
    auto* iter = result.iter();
    EXPECT_NE(iter, nullptr);
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}

TEST_F(QueryExecutorsTest, Aggregate) {
    std::string input = "input_agg";
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

    auto cmp = [] (const auto& lhs, const auto& rhs) {
        DCHECK_EQ(lhs.columns.size(), rhs.columns.size());
        for (size_t i = 0; i < lhs.columns.size(); ++i) {
            if (lhs.columns[i] < rhs.columns[i]) {
                return true;
            }
        }
        return false;
    };
    // group
    {
        qctx_->ectx()->setResult(input,
                    ExecResult::buildSequential(Value(ds), State()));
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
        agg->setInputVar(input);
        agg->setColNames(std::vector<std::string>{"col2"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        auto* iter = result.iter();
        EXPECT_NE(iter, nullptr);
        DataSet sortedDs = result.value().getDataSet();
        std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), cmp);
        EXPECT_EQ(sortedDs, expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
    // collect
    {
        qctx_->ectx()->setResult(input,
                    ExecResult::buildSequential(Value(ds), State()));
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
        // groupKeys.emplace_back(expr.get());
        groupItems.emplace_back(std::make_pair(expr.get(), kCollect));
        auto* plan = qctx_->plan();
        auto* agg = Aggregate::make(plan, nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(input);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->varName());
        auto* iter = result.iter();
        EXPECT_NE(iter, nullptr);
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
    }
}
}  // namespace graph
}  // namespace nebula
