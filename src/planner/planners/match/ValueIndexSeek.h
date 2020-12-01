/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_VALUEINDEXSEEK_H_
#define PLANNER_PLANNERS_MATCH_VALUEINDEXSEEK_H_

#include "planner/planners/match/StartVidFinder.h"

namespace nebula {
namespace graph {
/*
 * The ValueIndexSeek was designed to find if could get starting vids by value index.
 */
class ValueIndexSeek final : public StartVidFinder {
public:
    static std::unique_ptr<ValueIndexSeek> make() {
        return std::unique_ptr<ValueIndexSeek>(new ValueIndexSeek());
    }

    bool matchNode(NodeContext* nodeCtx) override;

    bool matchEdge(EdgeContext* edgeCtx) override;

    StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

    StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

private:
    ValueIndexSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCH_VALUEINDEXSEEK_H_
