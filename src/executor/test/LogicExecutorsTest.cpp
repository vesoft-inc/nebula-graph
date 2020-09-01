/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/expression/VariableExpression.h"
#include "context/QueryContext.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "executor/logic/StartExecutor.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class LogicExecutorsTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(LogicExecutorsTest, Start) {
    auto* start = StartNode::make(qctx_.get());
    auto startExe = std::make_unique<StartExecutor>(start, qctx_.get());
    auto f = startExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
}

TEST_F(LogicExecutorsTest, Loop) {
    std::string counter = "counter";
    qctx_->ectx()->setValue(counter, 0);
    // ++counter{0} <= 5
    auto condition = std::make_unique<RelationalExpression>(
                Expression::Kind::kRelLE,
                new UnaryExpression(
                        Expression::Kind::kUnaryIncr,
                        new VersionedVariableExpression(
                                new std::string(counter),
                                new ConstantExpression(0))),
                new ConstantExpression(static_cast<int32_t>(5)));
    auto* loop = Loop::make(qctx_.get(), nullptr, nullptr, condition.get());

    auto* start = StartNode::make(qctx_.get());
    auto startExe = std::make_unique<StartExecutor>(start, qctx_.get());
    auto loopExe = std::make_unique<LoopExecutor>(loop, qctx_.get(), startExe.get());
    for (size_t i = 0; i < 5; ++i) {
        auto f = loopExe->execute();
        auto status = std::move(f).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(loop->varName());
        auto& value = result.value();
        EXPECT_TRUE(value.isBool());
        EXPECT_TRUE(value.getBool());
    }

    auto f = loopExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(loop->varName());
    auto& value = result.value();
    EXPECT_TRUE(value.isBool());
    EXPECT_FALSE(value.getBool());
}

TEST_F(LogicExecutorsTest, Select) {
    {
        auto condition = std::make_unique<ConstantExpression>(true);
        auto* select = Select::make(qctx_.get(), nullptr, nullptr, nullptr, condition.get());

        auto* start = StartNode::make(qctx_.get());
        auto startExe = std::make_unique<StartExecutor>(start, qctx_.get());
        auto selectExe = std::make_unique<SelectExecutor>(
                select, qctx_.get(), startExe.get(), startExe.get());

        auto f = selectExe->execute();
        auto status = std::move(f).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(select->varName());
        auto& value = result.value();
        EXPECT_TRUE(value.isBool());
        EXPECT_TRUE(value.getBool());
    }
    {
        auto condition = std::make_unique<ConstantExpression>(false);
        auto* select = Select::make(qctx_.get(), nullptr, nullptr, nullptr, condition.get());

        auto* start = StartNode::make(qctx_.get());
        auto startExe = std::make_unique<StartExecutor>(start, qctx_.get());
        auto selectExe = std::make_unique<SelectExecutor>(
                select, qctx_.get(), startExe.get(), startExe.get());

        auto f = selectExe->execute();
        auto status = std::move(f).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(select->varName());
        auto& value = result.value();
        EXPECT_TRUE(value.isBool());
        EXPECT_FALSE(value.getBool());
    }
}
}  // namespace graph
}  // namespace nebula
