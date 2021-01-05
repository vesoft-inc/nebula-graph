/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/AssignExecutor.h"
#include "common/expression/VariableExpression.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class AssignExecutorTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(AssignExecutorTest, ConstExpression) {
    {
        std::string varName = "intVar";
        int val = 13;
        qctx_->symTable()->newVariable(varName);
        auto* expr = new ConstantExpression(val);

        auto* assign = Assign::make(qctx_.get(), nullptr, varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }
    {
        std::string varName = "floatVar";
        float val = 1.234563726090;
        qctx_->symTable()->newVariable(varName);
        auto* expr = new ConstantExpression(val);

        auto* assign = Assign::make(qctx_.get(), nullptr, varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isFloat());
        EXPECT_EQ(result.getFloat(), val);
    }
    {
        std::string varName = "stringVar";
        std::string val = "hello world";
        qctx_->symTable()->newVariable(varName);
        auto* expr = new ConstantExpression(val);

        auto* assign = Assign::make(qctx_.get(), nullptr, varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isStr());
        EXPECT_EQ(result.getStr(), val);
    }
}

TEST_F(AssignExecutorTest, VariableExpression) {
    // var1 = 13, var = var1 + 7
    std::string varName = "var";
    qctx_->symTable()->newVariable(varName);

    std::string varName1 = "var1";
    qctx_->symTable()->newVariable(varName1);
    {
        // var1 = 13
        int val = 13;
        auto* expr = new ConstantExpression(val);
        auto* assign = Assign::make(qctx_.get(), nullptr, varName1, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto result = qctx_->ectx()->getValue(varName1);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }

    auto* addExpr = new ArithmeticExpression(Expression::Kind::kAdd,
                                             new VariableExpression(new std::string(varName1)),
                                             new ConstantExpression(7));
    auto* assign = Assign::make(qctx_.get(), nullptr, varName, addExpr);
    auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
    auto future = assignExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto result = qctx_->ectx()->getValue(varName);
    EXPECT_TRUE(result.isInt());
    EXPECT_EQ(result.getInt(), 20);
}

TEST_F(AssignExecutorTest, VariableExpression1) {
    // var1 = 13, var = var1++
    std::string varName = "var";
    qctx_->symTable()->newVariable(varName);

    std::string varName1 = "var1";
    qctx_->symTable()->newVariable(varName1);
    {
        // var1 = 13
        int val = 13;
        auto* expr = new ConstantExpression(val);
        auto* assign = Assign::make(qctx_.get(), nullptr, varName1, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto result = qctx_->ectx()->getValue(varName1);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }
    // var = var1++
    auto* addExpr = new UnaryExpression(Expression::Kind::kUnaryIncr,
                                        new VariableExpression(new std::string(varName1)));
    auto* assign = Assign::make(qctx_.get(), nullptr, varName, addExpr);
    auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
    auto future = assignExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto result = qctx_->ectx()->getValue(varName);
    EXPECT_TRUE(result.isInt());
    EXPECT_EQ(result.getInt(), 14);
}

}   // namespace graph
}   // namespace nebula
