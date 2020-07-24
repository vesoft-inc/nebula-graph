/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/QueryContext.h"

#include "common/interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

void QueryContext::addProfilingData(int64_t planNodeId,
                                    const cpp2::ProfilingStats* profilingStats) {
    if (!planDescription_) return;
    auto found = planDescription_->node_index_map.find(planNodeId);
    DCHECK(found != planDescription_->node_index_map.end());
    auto idx = found->second;
    DCHECK(profilingStats != nullptr);
    auto& planNodeDesc = planDescription_->plan_node_descs[idx];
    if (planNodeDesc.get_profiles() == nullptr) {
        planNodeDesc.set_profiles({*profilingStats});
    } else {
        planNodeDesc.get_profiles()->emplace_back(*profilingStats);
    }
}

void QueryContext::fillPlanDescription() {
    DCHECK(ep_ != nullptr);
    ep_->fillPlanDescription(planDescription_.get());
}

}   // namespace graph
}   // namespace nebula
