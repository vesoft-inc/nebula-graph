/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/ProjectExecutor.h"

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
    std::string vids = "vids";
    DataSet ds;
    ds.colNames.emplace_back("_vid");
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.columns.emplace_back(i);
        ds.rows.emplace_back(std::move(row));
    }
    auto expected = ds;
    qctx_->ectx()->setResult(vids,
                ExecResult::buildSequential(Value(std::move(ds)), State()));

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(vids),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column);
    auto* plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setInputVar(vids);
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
}  // namespace graph
}  // namespace nebula
