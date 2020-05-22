/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "planner/ExecutionPlan.h"
#include "planner/Query.h"
#include "service/ExecutionContext.h"
#include "validator/ASTValidator.h"

namespace nebula {
namespace graph {

class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        session_ = new ClientSession(0);
        session_->setSpace("test", 0);
        ectx_ = std::make_unique<ExecutionContext>();
        plan_ = std::make_unique<ExecutionPlan>(ectx_.get());
        charsetInfo_ = CharsetInfo::instance();
        // TODO: Need AdHocSchemaManager here.
    }

    void TearDown() override {
        delete session_;
    }

protected:
    // some utils
    inline ::testing::AssertionResult toPlan(const std::string &query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) {
            return ::testing::AssertionFailure() << result.status().toString();
        }
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        if (!validateResult.ok()) {
            return ::testing::AssertionFailure() << validateResult.toString();
        }
        if (nullptr == plan_) {
            return ::testing::AssertionFailure() << "Null plan";
        }
        return ::testing::AssertionSuccess();
    }

protected:
    ClientSession *session_;
    meta::SchemaManager *schemaMng_;
    std::unique_ptr<ExecutionContext> ectx_;
    std::unique_ptr<ExecutionPlan> plan_;
    CharsetInfo *charsetInfo_;
};

}   // namespace graph
}   // namespace nebula
