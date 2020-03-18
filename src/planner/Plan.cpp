/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Plan.h"

#include "base/Logging.h"
#include "exec/ExecutionContext.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

Plan::Plan(const std::string &id) : id_(id), ectx_(std::make_unique<ExecutionContext>()) {
    LOG(DEBUG) << "Generate plan " << id;
}

}   // namespace graph
}   // namespace nebula
