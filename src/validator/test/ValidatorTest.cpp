/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class ValidatorTest : public ValidatorTestBase {
public:
    void SetUp() override {
        ValidatorTestBase::SetUp();
        qctx_ = buildContext();
    }

    void TearDown() override {
        ValidatorTestBase::TearDown();
        qctx_.reset();
    }

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qctx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qctx_->plan();
    }

protected:
    std::unique_ptr<QueryContext>              qctx_;
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    std::vector<const char*> kinds;
    kinds.reserve(plan.size());
    std::transform(plan.cbegin(), plan.cend(), std::back_inserter(kinds), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
    return os;
}

using PK = nebula::graph::PlanNode::Kind;

TEST_F(ValidatorTest, Subgraph) {
    std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
    };
    ASSERT_TRUE(checkResult("GET SUBGRAPH 3 STEPS FROM \"1\"", expected));
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
}  // namespace graph
}  // namespace nebula
