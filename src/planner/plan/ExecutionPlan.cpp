/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/ExecutionPlan.h"

#include "common/graph/Response.h"
#include "common/interface/gen-cpp2/graph_types.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/IdGenerator.h"

DECLARE_bool(enable_lifetime_optimize);

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(PlanNode* root) : id_(EPIdGenerator::instance().id()), root_(root) {}

ExecutionPlan::~ExecutionPlan() {}

uint64_t ExecutionPlan::makePlanNodeDesc(const PlanNode* node) {
    DCHECK(planDescription_ != nullptr);
    auto found = planDescription_->nodeIndexMap.find(node->id());
    if (found != planDescription_->nodeIndexMap.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDescription_->planNodeDescs.size();
    planDescription_->nodeIndexMap.emplace(node->id(), planNodeDescPos);
    planDescription_->planNodeDescs.emplace_back(std::move(*node->explain()));
    auto& planNodeDesc = planDescription_->planNodeDescs.back();
    planNodeDesc.profiles = std::make_unique<std::vector<ProfilingStats>>();

    if (node->kind() == PlanNode::Kind::kSelect) {
        auto select = static_cast<const Select*>(node);
        setPlanNodeDeps(select, &planNodeDesc);
        descBranchInfo(select->then(), true, select->id());
        descBranchInfo(select->otherwise(), false, select->id());
    } else if (node->kind() == PlanNode::Kind::kLoop) {
        auto loop = static_cast<const Loop*>(node);
        setPlanNodeDeps(loop, &planNodeDesc);
        descBranchInfo(loop->body(), true, loop->id());
    }

    for (size_t i = 0; i < node->numDeps(); ++i) {
        makePlanNodeDesc(node->dep(i));
    }

    return planNodeDescPos;
}

void ExecutionPlan::descBranchInfo(const PlanNode* node, bool isDoBranch, int64_t id) {
    auto pos = makePlanNodeDesc(node);
    auto info = std::make_unique<PlanNodeBranchInfo>();
    info->isDoBranch = isDoBranch;
    info->conditionNodeId = id;
    planDescription_->planNodeDescs[pos].branchInfo = std::move(info);
}

void ExecutionPlan::setPlanNodeDeps(const PlanNode* node, PlanNodeDescription* planNodeDesc) const {
    auto deps = std::make_unique<std::vector<int64_t>>();
    for (size_t i = 0; i < node->numDeps(); ++i) {
        deps->emplace_back(node->dep(i)->id());
    }
    planNodeDesc->dependencies = std::move(deps);
}

void ExecutionPlan::describe(PlanDescription* planDesc) {
    planDescription_ = DCHECK_NOTNULL(planDesc);
    planDescription_->optimize_time_in_us = optimizeTimeInUs_;
    planDescription_->format = explainFormat_;
    makePlanNodeDesc(root_);
}

void ExecutionPlan::addProfileStats(int64_t planNodeId, ProfilingStats&& profilingStats) {
    // return directly if not enable profile
    if (!planDescription_) return;

    auto found = planDescription_->nodeIndexMap.find(planNodeId);
    DCHECK(found != planDescription_->nodeIndexMap.end());
    auto idx = found->second;
    auto& planNodeDesc = planDescription_->planNodeDescs[idx];
    planNodeDesc.profiles->emplace_back(std::move(profilingStats));
}

void ExecutionPlan::analyze(QueryContext* qctx) const {
    // TODO(yee): remove this flag
    if (FLAGS_enable_lifetime_optimize) {
        root_->outputVarPtr()->setLastUser(-1);   // special for root
        analyzeLifetime(qctx, root_);
    }
}

void ExecutionPlan::analyzeLifetime(QueryContext* qctx, const PlanNode* root, bool inLoop) const {
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
                    needMultipleVersionsForInputNodes(qctx, leftVerVar.first);
                }
                auto rightVerVar = join->rightVar();
                if (rightVerVar.second != ExecutionContext::kLatestVersion) {
                    needMultipleVersionsForInputNodes(qctx, rightVerVar.first);
                }
                break;
            }
            case PlanNode::Kind::kConjunctPath:
            case PlanNode::Kind::kUnionAllVersionVar:
            case PlanNode::Kind::kDataCollect: {
                for (auto var : currentNode->inputVars()) {
                    needMultipleVersionsForInputNodes(qctx, var->name);
                }
                break;
            }
            default:
                break;
        }
    }
}

void ExecutionPlan::needMultipleVersionsForInputNodes(QueryContext* qctx,
                                                      const std::string& var) const {
    auto symTbl = qctx->symTable();
    auto variable = DCHECK_NOTNULL(symTbl->getVar(var));
    for (auto w : variable->writtenBy) {
        w->setInPlaceUpdate(false);
    }
}

}   // namespace graph
}   // namespace nebula
