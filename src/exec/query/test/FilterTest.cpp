/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/FilterExecutor.h"
#include "exec/query/test/QueryTestBase.h"
#include "exec/query/ProjectExecutor.h"

namespace nebula {
namespace graph {

class FilterTest : public QueryTestBase {
};

TEST_F(FilterTest, TestGetNeighbors_src_dst) {
    auto* plan = qctx_->plan();
    auto yieldSentence = getYieldSentence(
            "YIELD $^.person.name AS name WHERE study.start_year >= 2010");
    auto* filterNode = Filter::make(plan, nullptr, yieldSentence->where()->filter());
    filterNode->setInputVar("input_neighbor");
    filterNode->setOutputVar("filter_getNeighbor");

    auto filterExec = std::make_unique<FilterExecutor>(filterNode, qctx_.get());
    EXPECT_TRUE(filterExec->execute().get().ok());
    auto& filterResult = qctx_->ectx()->getResult(filterNode->varName());
    EXPECT_EQ(filterResult.state().stat(), State::Stat::kSuccess);

    filterNode->setInputVar("filter_getNeighbor");
    auto* project = Project::make(plan, nullptr, yieldSentence->yieldColumns());
    project->setInputVar(filterNode->varName());
    project->setColNames(std::vector<std::string>{"name"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto& proSesult = qctx_->ectx()->getResult(project->varName());

    DataSet expected({"name"});
    expected.emplace_back(Row({Value("Ann")}));
    expected.emplace_back(Row({Value("Ann")}));
    expected.emplace_back(Row({Value("Tom")}));

    EXPECT_EQ(proSesult.value().getDataSet(), expected);
    EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);
}

TEST_F(FilterTest, TestSequential) {
    auto* plan = qctx_->plan();
    auto yieldSentence = getYieldSentence(
            "YIELD $-.v_name AS name WHERE $-.e_start_year >= 2010");
    auto* filterNode = Filter::make(plan, nullptr, yieldSentence->where()->filter());
    filterNode->setInputVar("input_sequential");
    filterNode->setOutputVar("filter_sequential");

    auto filterExec = std::make_unique<FilterExecutor>(filterNode, qctx_.get());
    EXPECT_TRUE(filterExec->execute().get().ok());
    auto& filterResult = qctx_->ectx()->getResult(filterNode->varName());
    EXPECT_EQ(filterResult.state().stat(), State::Stat::kSuccess);

    filterNode->setInputVar("filter_sequential");
    auto* project = Project::make(plan,
                                  nullptr, yieldSentence->yieldColumns());
    project->setInputVar(filterNode->varName());
    project->setColNames(std::vector<std::string>{"name"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto& proSesult = qctx_->ectx()->getResult(project->varName());

    DataSet expected({"name"});
    expected.emplace_back(Row({Value("Ann")}));

    EXPECT_EQ(proSesult.value().getDataSet(), expected);
    EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);
}


TEST_F(FilterTest, TestEmpty) {
    auto* plan = qctx_->plan();
    auto yieldSentence = getYieldSentence(
            "YIELD $^.person.name AS name WHERE study.start_year >= 2010");
    auto* filterNode = Filter::make(plan, nullptr, yieldSentence->where()->filter());
    filterNode->setInputVar("empty");
    filterNode->setOutputVar("filter_empty");

    auto filterExec = std::make_unique<FilterExecutor>(filterNode, qctx_.get());
    EXPECT_TRUE(filterExec->execute().get().ok());
    auto& filterResult = qctx_->ectx()->getResult(filterNode->varName());
    EXPECT_EQ(filterResult.state().stat(), State::Stat::kSuccess);

    filterNode->setInputVar("filter_empty");
    auto* project = Project::make(plan, nullptr, yieldSentence->yieldColumns());
    project->setInputVar(filterNode->varName());
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
