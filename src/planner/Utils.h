/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "planner/Query.h"

namespace nebula {
namespace graph {

class PlanNodeUtils final {
public:
    static bool isSingleDependencyNode(const PlanNode* node);
};

}  // namespace graph
}  // namespace nebula
