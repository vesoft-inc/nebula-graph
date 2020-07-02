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

#define DEDUP_RESUTL_CHECK(inputName, outputName, sentence, expected)                          \
    do {                                                                                       \
        auto* plan = qctx_->plan();                                                            \
        auto yieldSentence = getYieldSentence(sentence);                                       \
        auto* dedupNode = Dedup::make(plan, nullptr);                                          \
        dedupNode->setInputVar(inputName);                                                     \
        dedupNode->setOutputVar(outputName);                                                   \
        auto dedupExec = std::make_unique<DedupExecutor>(dedupNode, qctx_.get());              \
        EXPECT_TRUE(dedupExec->execute().get().ok());                                          \
        auto& dedupResult = qctx_->ectx()->getResult(dedupNode->varName());                    \
        EXPECT_EQ(dedupResult.state().stat(), State::Stat::kSuccess);                          \
                                                                                               \
        dedupNode->setInputVar(outputName);                                                    \
        auto* project = Project::make(plan, nullptr, yieldSentence->yieldColumns());           \
        project->setInputVar(dedupNode->varName());                                            \
        project->setColNames(std::vector<std::string>{"name"});                                \
                                                                                               \
        auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());                 \
        EXPECT_TRUE(proExe->execute().get().ok());                                             \
        auto& proSesult = qctx_->ectx()->getResult(project->varName());                        \
                                                                                               \
        EXPECT_EQ(proSesult.value().getDataSet(), expected);                                   \
        EXPECT_EQ(proSesult.state().stat(), State::Stat::kSuccess);                            \
    } while (false)

TEST_F(DedupTest, TestSequential) {
    DataSet expected({"name"});
    expected.emplace_back(Row({Value("School1")}));

    DEDUP_RESUTL_CHECK("input_sequential",
                       "dedup_sequential",
                       "YIELD DISTINCT $-.v_dst as name",
                       expected);
}

TEST_F(DedupTest, TestEmpty) {
    DataSet expected({"name"});
    DEDUP_RESUTL_CHECK("empty",
                       "dedup_sequential",
                       "YIELD DISTINCT $-.v_dst as name",
                       expected);
}
}  // namespace graph
}  // namespace nebula
