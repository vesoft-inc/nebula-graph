/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_SEGMENTSCONNECTOR_H_
#define PLANNER_PLANNERS_MATCH_SEGMENTSCONNECTOR_H_

#include "context/QueryContext.h"
#include "planner/PlanNode.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
/**
 * The SegmentsConnector was designed to be a util to help connecting the
 * plan segment.
 */
class SegmentsConnector final {
public:
    enum class ConnectStrategy : int8_t {
        kAddDependency,
        kInnerJoin,
        kLeftOuterJoin,
        kCartesianProduct,
        kUnion,
    };

    SegmentsConnector() = delete;

    static StatusOr<SubPlan> connectSegments(SubPlan& left, SubPlan& right) {
        UNUSED(left);
        UNUSED(right);
        // Analyse the relation of two segments and connect them.
        return Status::Error("TODO");
    }

    static PlanNode* innerJoinSegments(QueryContext* qctx,
                                       const PlanNode* left,
                                       const PlanNode* right);

    static void addDependency(const PlanNode* left, const PlanNode* right);
};

class SegmentsConnectStrategy {
public:
    explicit SegmentsConnectStrategy(QueryContext* qctx) : qctx_(qctx) {}

    virtual ~SegmentsConnectStrategy() = default;

    virtual PlanNode* connect(const PlanNode* left, const PlanNode* right) = 0;

protected:
    QueryContext*   qctx_;
};
}  // namespace graph
}  // namespace nebula
#endif
