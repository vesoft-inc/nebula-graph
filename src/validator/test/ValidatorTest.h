/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "validator/test/MockSchemaManager.h"
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
class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        schemaMng_ = MockSchemaManager::make_unique();
        qCtx_ = buildContext();
    }

    void TearDown() override {
    }

    std::unique_ptr<QueryContext> buildContext();

protected:
    ::testing::AssertionResult verifyPlan(const PlanNode* root,
                                          const std::vector<PlanNode::Kind>& expected) const {
        if (root == nullptr) {
            return ::testing::AssertionFailure() << "Get nullptr plan.";
        }

        std::vector<PlanNode::Kind> result;
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        } else {
            return ::testing::AssertionFailure()
                        << "\n"
                        << "     Result: " << result << "\n"
                        << "     Expected: " << expected;
        }
    }

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qCtx_->plan();
    }

    static void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result);

    std::shared_ptr<ClientSession>         session_;
    std::unique_ptr<meta::SchemaManager>   schemaMng_;
    std::unique_ptr<QueryContext>          qCtx_;
};

inline std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    std::vector<const char*> kinds;
    kinds.reserve(plan.size());
    std::transform(plan.cbegin(), plan.cend(), std::back_inserter(kinds), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
    return os;
}

}  // namespace graph
}  // namespace nebula
