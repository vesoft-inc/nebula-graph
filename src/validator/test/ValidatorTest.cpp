/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"
<<<<<<< HEAD
#include "validator/test/MockSchemaManager.h"
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
=======
>>>>>>> Support DML,DDL to use inputNode

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;

class ValidatorTest : public ValidatorTestBase {
<<<<<<< HEAD
<<<<<<< HEAD
public:
    static void SetUpTestCase() {
        auto sessionId = 0;
        auto session = new ClientSession(sessionId);
        auto spaceName = "test_space";
        auto spaceId = 1;
        session->setSpace(spaceName, spaceId);
        session_.reset(session);
        auto* sm = new MockSchemaManager();
        sm->init();
        schemaMng_.reset(sm);
    }

    void SetUp() override {
=======
protected:
    void SetUp() override {
        ValidatorTestBase::SetUp();
>>>>>>> rebase upstream
        qctx_ = buildContext();
    }

    void TearDown() override {
<<<<<<< HEAD
        qctx_.reset();
    }

    std::unique_ptr<QueryContext> buildContext();

=======
        ValidatorTestBase::TearDown();
        qctx_.reset();
    }
>>>>>>> rebase upstream
    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qctx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qctx_->plan();
    }

protected:
<<<<<<< HEAD
    static std::shared_ptr<ClientSession>           session_;
    static std::unique_ptr<meta::SchemaManager>     schemaMng_;
    std::unique_ptr<QueryContext>                   qctx_;
};

std::shared_ptr<ClientSession> ValidatorTest::session_;
std::unique_ptr<meta::SchemaManager> ValidatorTest::schemaMng_;

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_.get());
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
=======
=======
    std::unique_ptr<QueryContext>              qctx_;
>>>>>>> rebase upstream
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    std::vector<const char*> kinds;
    kinds.reserve(plan.size());
    std::transform(plan.cbegin(), plan.cend(), std::back_inserter(kinds), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
    return os;
>>>>>>> Support DML,DDL to use inputNode
}

using PK = nebula::graph::PlanNode::Kind;
TEST_F(ValidatorTest, Subgraph) {
<<<<<<< HEAD
    {
        auto status = validate("GET SUBGRAPH 3 STEPS FROM \"1\"");
        ASSERT_TRUE(status.ok());
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
=======
    std::vector<PlanNode::Kind> expected = {
        PK::kLoop,
        PK::kStart,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kStart,
    };
    ASSERT_TRUE(checkResult("GET SUBGRAPH 3 STEPS FROM \"1\"", expected));
>>>>>>> Support DML,DDL to use inputNode
}

TEST_F(ValidatorTest, TestFirstSentence) {
    auto testFirstSentence = [](StatusOr<ExecutionPlan*> so) -> bool {
        if (so.ok()) return false;
        auto status = std::move(so).status();
        auto err = status.toString();
        return err.find_first_of("SyntaxError: Could not start with the statement") == 0;
    };

    {
        auto status = validate("LIMIT 2, 10");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("LIMIT 2, 10 | YIELD 2");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("LIMIT 2, 10 | YIELD 2 | YIELD 3");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("ORDER BY 1");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("GROUP BY 1");
        ASSERT_TRUE(testFirstSentence(status));
    }
}
<<<<<<< HEAD

TEST_F(ValidatorTest, TestSpace) {
    {
        std::string query = "CREATE SPACE TEST";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        auto plan = context->plan();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kCreateSpace,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}

TEST_F(ValidatorTest, Go) {
    {
        std::string query = "GO FROM \"1\" OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO 3 STEPS FROM \"1\" OVER like";
        auto status = validate(query);
        // TODO: implement n steps and test plan.
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER like REVERSELY";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like.start";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.person.name,$^.person.age";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name,$$.person.age";
        auto status = validate(query);
        // TODO: implement get dst props and test plan.
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE like.likeness > 90";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        // TODO
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.name == \"me\"";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        // TODO
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}

TEST_F(ValidatorTest, GoInvalid) {
    {
        // friend not exist.
        std::string query = "GO FROM \"1\" OVER friend";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.manager.name,$^.person.age";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.manager.name,$$.person.age";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // column not exist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.col OVER like";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // invalid id type
        std::string query = "GO FROM \"1\" OVER like YIELD like.likeness AS id"
                            "| GO FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // multi inputs
        std::string query = "$var = GO FROM \"2\" OVER like;"
                            "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like WHERE $var.id == \"\"";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
}
=======
>>>>>>> Support DML,DDL to use inputNode
}  // namespace graph
}  // namespace nebula
