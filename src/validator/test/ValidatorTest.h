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
#include "planner/PlanNode.h"
#include "planner/Admin.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        // TODO: Need AdHocSchemaManager here.
    }

    void SetUp() override {
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
        result.emplace_back(root->kind());
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        } else {
            return ::testing::AssertionFailure()
                        << "\n"
                        << "     Result: " << printPlan(result) << "\n"
                        << "     Expected: " << printPlan(expected);
        }
    }

    std::string printPlan(const std::vector<PlanNode::Kind>& plan) const {
        std::stringstream ss;
        ss << "[";
        for (auto& kind : plan) {
            ss << kind << ", ";
        }
        ss << "]";
        return ss.str();
    }

    void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) const;

    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
};

}  // namespace graph
}  // namespace nebula
