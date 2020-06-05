/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include <folly/init/Init.h>
#include <folly/stop_watch.h>
#include <gtest/gtest.h>

#include "exec/ExecutionError.h"
#include "exec/Executor.h"
#include "planner/Query.h"
#include "context/QueryContext.h"
#include "schedule/Scheduler.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace nebula {
namespace graph {

class ExecutionPlanTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        plan_ = qctx_->plan();
    }

    void cleanup() {
    }

    void run() {
        ASSERT_NE(plan_->root(), nullptr);


        watch_.reset();
        Scheduler(qctx_.get())
            .schedule()
            .then([](Status s) { ASSERT_TRUE(s.ok()) << s.toString(); })
            .onError([](const ExecutionError& e) { LOG(INFO) << e.what(); })
            .onError([](const std::exception& e) { LOG(INFO) << "exception: " << e.what(); })
            .ensure([this]() {
                auto us = duration_cast<microseconds>(watch_.elapsed());
                LOG(INFO) << "elapsed time: " << us.count() << "us";
                cleanup();
            });
    }

protected:
    folly::stop_watch<> watch_;
    ExecutionPlan* plan_;
    std::unique_ptr<QueryContext>  qctx_;
};

TEST_F(ExecutionPlanTest, TestSimplePlan) {
    auto start = StartNode::make(plan_);
    Expression* expr = nullptr;
    auto filter = Filter::make(plan_, start, expr);
    auto dedup = Dedup::make(plan_, filter, nullptr);
    YieldColumns* cols = nullptr;
    auto proj = Project::make(plan_, dedup, cols);
    plan_->setRoot(proj);

    run();
}

TEST_F(ExecutionPlanTest, TestSelect) {
    auto start = StartNode::make(plan_);
    auto thenStart = StartNode::make(plan_);
    auto filter = Filter::make(plan_, thenStart, nullptr);
    auto elseStart = StartNode::make(plan_);
    auto project = Project::make(plan_, elseStart, nullptr);
    auto select = Selector::make(plan_, start, filter, project, nullptr);
    auto output = Project::make(plan_, select, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestLoopPlan) {
    auto start = StartNode::make(plan_);
    auto bodyStart = StartNode::make(plan_);
    auto filter = Filter::make(plan_, bodyStart, nullptr);
    auto loop = Loop::make(plan_, start, filter, nullptr);
    auto project = Project::make(plan_, loop, nullptr);

    plan_->setRoot(project);

    run();
}

TEST_F(ExecutionPlanTest, TestMultiOutputs) {
    auto start = StartNode::make(plan_);
    auto mout = MultiOutputsNode::make(plan_, start);
    auto filter = Filter::make(plan_, mout, nullptr);
    auto project = Project::make(plan_, mout, nullptr);
    auto uni = Union::make(plan_, filter, project);
    auto output = Project::make(plan_, uni, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestMultiOutputsInLoop) {
    auto loopStart = StartNode::make(plan_);
    auto mout = MultiOutputsNode::make(plan_, loopStart);
    auto filter = Filter::make(plan_, mout, nullptr);
    auto project = Project::make(plan_, mout, nullptr);
    auto uni = Union::make(plan_, filter, project);
    auto loopEnd = Project::make(plan_, uni, nullptr);
    auto start = StartNode::make(plan_);
    auto loop = Loop::make(plan_, start, loopEnd, nullptr);
    auto end = Project::make(plan_, loop, nullptr);

    plan_->setRoot(end);

    run();
}

}   // namespace graph
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
