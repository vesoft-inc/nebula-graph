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
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        } else {
            return ::testing::AssertionFailure() << "\n     Result: " << printPlan(result)
                                                 << "\n     Expected: " << printPlan(expected);
        }
    }

    std::string printPlan(const std::vector<PlanNode::Kind>& plan) const {
        std::vector<const char*> kinds;
        kinds.reserve(plan.size());
        std::transform(plan.cbegin(), plan.cend(), kinds.begin(), PlanNode::toString);
        std::stringstream ss;
        ss << "[" << folly::join(", ", kinds) << "]";
        return ss.str();
    }

    void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) const {
        std::queue<const PlanNode*> queue;
        std::unordered_set<int64_t> visited;
        queue.emplace(root);

        while (!queue.empty()) {
            auto node = queue.front();
            queue.pop();
            if (visited.find(node->id()) != visited.end()) {
                continue;
            }
            visited.emplace(node->id());
            result.emplace_back(node->kind());

            switch (node->kind()) {
                case PlanNode::Kind::kUnknown:
                    ASSERT_TRUE(false) << "Unknown Plan Node.";
                case PlanNode::Kind::kStart: {
                    break;
                }
                case PlanNode::Kind::kGetNeighbors:
                case PlanNode::Kind::kGetVertices:
                case PlanNode::Kind::kGetEdges:
                case PlanNode::Kind::kReadIndex:
                case PlanNode::Kind::kFilter:
                case PlanNode::Kind::kProject:
                case PlanNode::Kind::kSort:
                case PlanNode::Kind::kLimit:
                case PlanNode::Kind::kAggregate:
                case PlanNode::Kind::kSwitchSpace:
                case PlanNode::Kind::kMultiOutputs:
                case PlanNode::Kind::kDedup: {
                    auto* current = static_cast<const SingleInputNode*>(node);
                    queue.emplace(current->input());
                    break;
                }
                case PlanNode::Kind::kCreateSpace:
                case PlanNode::Kind::kCreateTag:
                case PlanNode::Kind::kCreateEdge:
                case PlanNode::Kind::kDescSpace:
                case PlanNode::Kind::kDescTag:
                case PlanNode::Kind::kDescEdge:
                case PlanNode::Kind::kInsertVertices:
                case PlanNode::Kind::kInsertEdges: {
                    // TODO: DDLs and DMLs are kind of single input node.
                }
                case PlanNode::Kind::kUnion:
                case PlanNode::Kind::kIntersect:
                case PlanNode::Kind::kMinus: {
                    auto* current = static_cast<const BiInputNode*>(node);
                    queue.emplace(current->left());
                    queue.emplace(current->right());
                    break;
                }
                case PlanNode::Kind::kSelector: {
                    auto* current = static_cast<const Selector*>(node);
                    queue.emplace(current->input());
                    queue.emplace(current->then());
                    if (current->otherwise() != nullptr) {
                        queue.emplace(current->otherwise());
                    }
                    break;
                }
                case PlanNode::Kind::kLoop: {
                    auto* current = static_cast<const Loop*>(node);
                    queue.emplace(current->input());
                    queue.emplace(current->body());
                    break;
                }
                default:
                    LOG(FATAL) << "Unknown PlanNode: " << static_cast<int64_t>(node->kind());
            }
        }
    }

protected:
    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
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
        std::string query = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        auto plan = context->plan();
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

}  // namespace graph
}  // namespace nebula
