/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/QueryContext.h"

namespace nebula {
namespace graph {

void QueryContext::addProfilingData(int64_t planNodeId) {
    auto found = planNodeIndexMap_.find(planNodeId);
    DCHECK_NE(found, planNodeIndexMap_.end());
    auto idx = found->second;
    UNUSED(idx);
}

}   // namespace graph
}   // namespace nebula
