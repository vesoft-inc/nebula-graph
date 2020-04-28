/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include "exec/Executor.h"
#include "planner/PlanNode.h"
#include "service/ExecutionContext.h"
#include "util/IdGenerator.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(ExecutionContext* ectx)
    : id_(IdGenerator::get(IdGenerator::Type::SEQUENCE)->id()),
      ectx_(ectx) {
    DCHECK_NOTNULL(ectx);
}

ExecutionPlan::~ExecutionPlan() {
    ectx_ = nullptr;
}

PlanNode* ExecutionPlan::addPlanNode(PlanNode* node) {
    node->setId(IdGenerator::get(IdGenerator::Type::SEQUENCE)->id());
    return ectx_->objPool()->add(node);
}

Executor* ExecutionPlan::createExecutor() {
    std::unordered_map<int64_t, Executor*> cache;
    return Executor::makeExecutor(root_, ectx_, &cache);
}

}   // namespace graph
}   // namespace nebula
