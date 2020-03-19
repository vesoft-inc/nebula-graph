/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/Executor.h"

#include "exec/ExecutionContext.h"
#include "exec/FilterExecutor.h"
#include "exec/LoopExecutor.h"
#include "exec/SelectExecutor.h"
#include "exec/StartExecutor.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

// static
Executor *Executor::makeExecutor(const PlanNode *node,
                                 ExecutionContext *ectx,
                                 ObjectPool *objPool) {
    switch (node->kind()) {
        case PlanNode::Kind::kAggregate:
            break;
        case PlanNode::Kind::kSort:
            break;
        case PlanNode::Kind::kFilter: {
            auto *filter = static_cast<const Filter *>(node);
            auto *input = makeExecutor(filter->children()[0].get(), ectx, objPool);
            return objPool->add(new FilterExecutor(node, ectx, input));
        }
        case PlanNode::Kind::kGetEdges:
            break;
        case PlanNode::Kind::kGetVertices:
            break;
        case PlanNode::Kind::kGetNeighbors:
            break;
        case PlanNode::Kind::kLimit:
            break;
        case PlanNode::Kind::kProject:
            break;
        case PlanNode::Kind::kReadIndex:
            break;
        case PlanNode::Kind::kStart:
            return objPool->add(new StartExecutor(node, ectx));
        case PlanNode::Kind::kUnion:
            break;
        case PlanNode::Kind::kIntersect:
            break;
        case PlanNode::Kind::kMinus:
            break;
        case PlanNode::Kind::kLoop: {
            auto *loop = static_cast<const Loop *>(node);
            auto *input = makeExecutor(loop->children()[0].get(), ectx, objPool);
            auto *body = makeExecutor(loop->loopBody().get(), ectx, objPool);
            auto *exit = makeExecutor(loop->exitBranch().get(), ectx, objPool);
            return objPool->add(new LoopExecutor(node, ectx, input, body, exit));
        }
        case PlanNode::Kind::kSelector: {
            auto *select = static_cast<const Selector *>(node);
            auto *input = makeExecutor(select->children()[0].get(), ectx, objPool);
            auto *then = makeExecutor(select->thenBranch().get(), ectx, objPool);
            auto *els = makeExecutor(select->elseBranch().get(), ectx, objPool);
            return objPool->add(new SelectExecutor(node, ectx, input, then, els));
        }
        case PlanNode::Kind::kUnknown:
        default:
            LOG(FATAL) << "Unknown plan node kind.";
            break;
    }
    return nullptr;
}

Status Executor::finish(std::list<cpp2::Row> dataset) {
    return ectx_->addExecutorResult(folly::stringPrintf("%s-%lu", name_.c_str(), id_),
                                    std::move(dataset));
}

}   // namespace graph
}   // namespace nebula
