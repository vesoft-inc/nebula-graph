/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "validator/test/ValidatorTest.h"
#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {

/*static*/
void ValidatorTest::bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) {
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
            case PlanNode::Kind::kDedup:
            case PlanNode::Kind::kDataCollect: {
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

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_.get());
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
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
