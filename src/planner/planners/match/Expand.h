/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_EXPAND_H_
#define PLANNER_PLANNERS_MATCH_EXPAND_H_

namespace nebula {
namespace graph {
/*
 * The Expand was designed to handle the pattern expanding.
 */
class Expand final {
public:
    Expand() = default;

    static Status expand(SubPlan& plan) {
        UNUSED(plan);
        return Status::Error("TODO");
    }
};
}  // namespace graph
}  // namespace nebula
#endif
