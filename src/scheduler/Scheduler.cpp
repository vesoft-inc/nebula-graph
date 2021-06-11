/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

/*static*/ void Scheduler::analyzeLifetime(const PlanNode *node, QueryContext *qctx, bool inLoop) {
    for (auto& inputVar : node->inputVars()) {
        if (inputVar != nullptr) {
            inputVar->setLastUser((node->kind() == PlanNode::Kind::kLoop || inLoop) ?
                                  -1 :
                                  node->id());
        }
    }
    switch (node->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<const Select *>(node);
            analyzeLifetime(sel->then(), qctx, inLoop);
            analyzeLifetime(sel->otherwise(), qctx, inLoop);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop *>(node);
            loop->outputVarPtr()->setLastUser(-1);
            analyzeLifetime(loop->body(), qctx, true);
            break;
        }
        default:
            break;
    }

    for (auto dep : node->dependencies()) {
        analyzeLifetime(dep, qctx, inLoop);
    }
}

}   // namespace graph
}   // namespace nebula
