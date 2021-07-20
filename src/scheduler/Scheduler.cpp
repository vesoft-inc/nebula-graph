/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/ExecutionContext.h"
#include "context/QueryContext.h"
#include "context/Symbols.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/plan/Algo.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

Scheduler::Scheduler(QueryContext* qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

void Scheduler::analyzeLifetime(const PlanNode* root, bool inLoop) const {
    std::stack<std::tuple<const PlanNode*, bool>> stack;
    stack.push(std::make_tuple(root, inLoop));
    while (!stack.empty()) {
        const auto& current = stack.top();
        auto currentNode = std::get<0>(current);
        auto currentInLoop = std::get<1>(current);
        for (auto& inputVar : currentNode->inputVars()) {
            if (inputVar != nullptr) {
                auto isLoopOrBody = currentNode->kind() == PlanNode::Kind::kLoop || currentInLoop;
                inputVar->setLastUser(isLoopOrBody ? -1 : currentNode->id());
            }
        }
        stack.pop();

        for (auto dep : currentNode->dependencies()) {
            stack.push(std::make_tuple(dep, currentInLoop));
        }
        switch (currentNode->kind()) {
            case PlanNode::Kind::kSelect: {
                auto sel = static_cast<const Select*>(currentNode);
                stack.push(std::make_tuple(sel->then(), currentInLoop));
                stack.push(std::make_tuple(sel->otherwise(), currentInLoop));
                break;
            }
            case PlanNode::Kind::kLoop: {
                auto loop = static_cast<const Loop*>(currentNode);
                loop->outputVarPtr()->setLastUser(-1);
                stack.push(std::make_tuple(loop->body(), true));
                break;
            }
            case PlanNode::Kind::kInnerJoin:
            case PlanNode::Kind::kLeftJoin: {
                auto join = static_cast<const Join*>(currentNode);
                auto leftVerVar = join->leftVar();
                if (leftVerVar.second != ExecutionContext::kLatestVersion) {
                    needMultipleVersionsForInputNodes(leftVerVar.first);
                }
                auto rightVerVar = join->rightVar();
                if (rightVerVar.second != ExecutionContext::kLatestVersion) {
                    needMultipleVersionsForInputNodes(rightVerVar.first);
                }
                break;
            }
            case PlanNode::Kind::kConjunctPath:
            case PlanNode::Kind::kUnionAllVersionVar:
            case PlanNode::Kind::kDataCollect: {
                for (auto var : currentNode->inputVars()) {
                    needMultipleVersionsForInputNodes(var->name);
                }
                break;
            }
            default:
                break;
        }
    }
}

void Scheduler::needMultipleVersionsForInputNodes(const std::string& var) const {
    auto symTbl = qctx_->symTable();
    auto variable = DCHECK_NOTNULL(symTbl->getVar(var));
    for (auto w : variable->writtenBy) {
        w->setInPlaceUpdate(false);
    }
}

}   // namespace graph
}   // namespace nebula
