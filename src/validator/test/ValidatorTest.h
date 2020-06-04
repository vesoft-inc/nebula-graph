/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        qCtx_ = buildContext();
    }

    void TearDown() override {
    }

protected:
    // some utils
    inline ::testing::AssertionResult toPlan(const std::string &query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) {
            return ::testing::AssertionFailure() << result.status().toString();
        }
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        if (!validateResult.ok()) {
            return ::testing::AssertionFailure() << validateResult.toString();
        }
        return ::testing::AssertionSuccess();
    }

    std::unique_ptr<QueryContext> buildContext() {
        auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
        rctx->setSession(session_);
        auto qctx = std::make_unique<QueryContext>();
        qctx->setRctx(std::move(rctx));
        qctx->setSchemaManager(schemaMng_);
        qctx->setCharsetInfo(CharsetInfo::instance());
        return qctx;
    }


protected:
    std::shared_ptr<ClientSession>      session_;
    meta::SchemaManager*                schemaMng_;
    std::unique_ptr<QueryContext>       qCtx_;
};

}  // namespace graph
}  // namespace nebula
