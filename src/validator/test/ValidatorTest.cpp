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

void ValidatorTest::bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) const {
    switch (root->kind()) {
        case PlanNode::Kind::kUnknown:
            ASSERT_TRUE(false) << "Unkown Plan Node.";
        case PlanNode::Kind::kStart: {
            return;
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
        case PlanNode::Kind::kDedup: {
            auto* current = static_cast<const SingleInputNode*>(root);
            result.emplace_back(current->input()->kind());
            bfsTraverse(current->input(), result);
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
            auto* current = static_cast<const BiInputNode*>(root);
            result.emplace_back(current->left()->kind());
            result.emplace_back(current->right()->kind());
            bfsTraverse(current->left(), result);
            bfsTraverse(current->right(), result);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto* current = static_cast<const Select*>(root);
            result.emplace_back(current->input()->kind());
            result.emplace_back(current->then()->kind());
            if (current->otherwise() != nullptr) {
                result.emplace_back(current->otherwise()->kind());
            }
            bfsTraverse(current->input(), result);
            bfsTraverse(current->then(), result);
            if (current->otherwise() != nullptr) {
                bfsTraverse(current->otherwise(), result);
            }
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto* current = static_cast<const Loop*>(root);
            result.emplace_back(current->input()->kind());
            result.emplace_back(current->body()->kind());
            bfsTraverse(current->input(), result);
            bfsTraverse(current->body(), result);
            break;
        }
        default:
            LOG(FATAL) << "Unkown PlanNode: " << static_cast<int64_t>(root->kind());
    }
}

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_);
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
}

std::shared_ptr<ClientSession>      ValidatorTest::session_;
meta::SchemaManager*                ValidatorTest::schemaMng_;

}  // namespace graph
}  // namespace nebula
