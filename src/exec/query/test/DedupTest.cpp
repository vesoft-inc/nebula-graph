/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/DedupExecutor.h"
#include "exec/query/test/QueryTestBase.h"
#include "exec/query/ProjectExecutor.h"

namespace nebula {
namespace graph {

class DedupTest : public QueryTestBase {
};

TEST_F(DedupTest, TestGetNeighbors_src_dst) {
    auto* plan = qctx_->plan();
    auto yieldColumns = getYieldColumns(
            "YIELD DISTINCT $^.person.age AS age, study._dst as dst");
    auto* dedupNode = Dedup::make(plan, nullptr, getExprs(yieldColumns));
    dedupNode->setInputVar("input_neighbor");
    dedupNode->setOutputVar("dedup_getNeighbor");

    auto defupExec = std::make_unique<DedupExecutor>(dedupNode, qctx_.get());
    EXPECT_TRUE(defupExec->execute().get().ok());
    auto& dedupResult = qctx_->ectx()->getResult(dedupNode->varName());
    EXPECT_EQ(dedupResult.state().stat(), State::Stat::kSuccess);

    dedupNode->setInputVar("dedup_getNeighbor");
    auto* project = Project::make(plan, nullptr, yieldColumns);
    project->setInputVar(dedupNode->varName());
    project->setColNames(std::vector<std::string>{"age", "dst"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto& proSesult = qctx_->ectx()->getResult(project->varName());

    DataSet expected({"age", "dst"});
    expected.emplace_back(Row({Value(18), Value("School1")}));
    expected.emplace_back(Row({Value(18), Value("School2")}));

    EXPECT_EQ(proSesult.value().getDataSet(), expected);
    EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);
}

TEST_F(DedupTest, TestSequential) {
    auto* plan = qctx_->plan();
    auto yieldColumns = getYieldColumns("YIELD DISTINCT $-.v_dst as name");
    auto* dedupNode = Dedup::make(plan, nullptr, getExprs(yieldColumns));
    dedupNode->setInputVar("input_sequential");
    dedupNode->setOutputVar("filter_sequential");

    auto dedupExec = std::make_unique<DedupExecutor>(dedupNode, qctx_.get());
    EXPECT_TRUE(dedupExec->execute().get().ok());
    auto& dedupResult = qctx_->ectx()->getResult(dedupNode->varName());
    EXPECT_EQ(dedupResult.state().stat(), State::Stat::kSuccess);

    dedupNode->setInputVar("filter_sequential");
    auto* project = Project::make(plan, nullptr, yieldColumns);
    project->setInputVar(dedupNode->varName());
    project->setColNames(std::vector<std::string>{"name"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto& proSesult = qctx_->ectx()->getResult(project->varName());

    DataSet expected({"name"});
    expected.emplace_back(Row({Value("School1")}));

    EXPECT_EQ(proSesult.value().getDataSet(), expected);
    EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);
}


TEST_F(DedupTest, TestEmpty) {
    auto* plan = qctx_->plan();
    auto yieldColumns = getYieldColumns("YIELD DISTINCT $^.person.name AS name, study._dst as dst");
    auto* dedupNode = Dedup::make(plan, nullptr, getExprs(yieldColumns));
    dedupNode->setInputVar("empty");
    dedupNode->setOutputVar("dedup_empty");

    auto dedupExec = std::make_unique<DedupExecutor>(dedupNode, qctx_.get());
    EXPECT_TRUE(dedupExec->execute().get().ok());
    auto& dedupResult = qctx_->ectx()->getResult(dedupNode->varName());
    EXPECT_EQ(dedupResult.state().stat(), State::Stat::kSuccess);

    dedupNode->setInputVar("dedup_empty");
    auto* project = Project::make(plan, nullptr, yieldColumns);
    project->setInputVar(dedupNode->varName());
    project->setColNames(std::vector<std::string>{"name"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto& proSesult = qctx_->ectx()->getResult(project->varName());

    DataSet expected({"name"});
    EXPECT_EQ(proSesult.value().getDataSet(), expected);
    EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);
}
}  // namespace graph
}  // namespace nebula
