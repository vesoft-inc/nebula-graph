/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"
#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"
#include "planner/PlanNode.h"
#include "planner/Admin.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class ValidatorTest : public ValidatorTestBase {
public:
    static void SetUpTestCase() {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        // TODO: Need AdHocSchemaManager here.
    }

    void SetUp() override {
        qctx_ = buildContext();
    }

    void TearDown() override {
        qctx_.reset();
    }

    std::unique_ptr<QueryContext> buildContext();

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qctx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qctx_->plan();
    }

    bool testFirstSentence(const std::string& err) const {
        return err.find_first_of("SyntaxError: Could not start with the statement") == 0;
    }

protected:
    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
    std::unique_ptr<QueryContext>              qctx_;
};

std::shared_ptr<ClientSession> ValidatorTest::session_;
meta::SchemaManager* ValidatorTest::schemaMng_;

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_);
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
}


TEST_F(ValidatorTest, Subgraph) {
    {
        auto status = validate("GET SUBGRAPH 3 STEPS FROM \"1\"");
        ASSERT_TRUE(status.ok());
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        using PK = nebula::graph::PlanNode::Kind;
        std::vector<PlanNode::Kind> expected = {
            PK::kLoop,
            PK::kStart,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}

TEST_F(ValidatorTest, TestFirstSentence) {
    {
        auto status = validate("LIMIT 2, 10");
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(testFirstSentence(std::move(status).status().toString()));
    }
    {
        auto status = validate("LIMIT 2, 10 | LIMIT 2");
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(testFirstSentence(std::move(status).status().toString()));
    }
    {
        auto status = validate("ORDER BY 1");
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(testFirstSentence(std::move(status).status().toString()));
    }
    {
        auto status = validate("GROUP BY 1");
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(testFirstSentence(std::move(status).status().toString()));
    }
}

}  // namespace graph
}  // namespace nebula
