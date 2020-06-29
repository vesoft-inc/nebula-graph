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
#include "validator/test/MockSchemaManager.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        auto session = new ClientSession(0);
        session->setSpace("test", 1);
        session_.reset(session);
        schemaMng_ = CHECK_NOTNULL(MockSchemaManager::make_unique());
        qCtx_ = buildContext();
        expectedQueryCtx_ = buildContext();
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

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qCtx_->plan();
    }


    std::unique_ptr<QueryContext> buildContext() {
        auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
        rctx->setSession(session_);
        auto qctx = std::make_unique<QueryContext>();
        qctx->setRctx(std::move(rctx));
        qctx->setSchemaManager(schemaMng_.get());
        qctx->setCharsetInfo(CharsetInfo::instance());
        return qctx;
    }


    static Status EqSelf(const PlanNode* l, const PlanNode* r);

    static Status Eq(const PlanNode *l, const PlanNode *r);

    static ::testing::AssertionResult verifyPlan(const PlanNode* root,
                                                 const std::vector<PlanNode::Kind>& expected) {
        if (root == nullptr) {
            return ::testing::AssertionFailure() << "Get nullptr plan.";
        }

        std::vector<PlanNode::Kind> result;
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << "\n\tResult: " << result << "\n\tExpected: " << expected;
    }

    static void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) {
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
                case PlanNode::Kind::kSelect: {
                    auto* current = static_cast<const Select*>(node);
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
    std::shared_ptr<ClientSession>       session_;
    std::unique_ptr<meta::SchemaManager> schemaMng_;
    std::unique_ptr<QueryContext>        qCtx_;
    // used to hold the expected query plan
    std::unique_ptr<QueryContext>        expectedQueryCtx_;
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
