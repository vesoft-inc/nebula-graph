/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/QueryContext.h"

#include "common/interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

void QueryContext::addProfilingData(int64_t planNodeId, cpp2::ProfilingStats profilingStats) {
    if (!planDescription_) return;
    auto found = planNodeIndexMap_.find(planNodeId);
    DCHECK(found != planNodeIndexMap_.end());
    auto idx = found->second;
    planDescription_->plan_node_descs[idx].get_profiles()->emplace_back(std::move(profilingStats));
}

void QueryContext::fillPlanDescription() {
    DCHECK_NOTNULL(ep_)->fillPlanDescription(planDescription_.get(), &planNodeIndexMap_);
}

}   // namespace graph
}   // namespace nebula
