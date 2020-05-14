/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include <cstdint>
#include <memory>
#include "expression/Expression.h"
#include "context/QueryContext.h"

#include <folly/futures/Future.h>

#include "common/base/Status.h"

namespace nebula {

class ObjectPool;

namespace graph {

class Executor;
class IdGenerator;
class PlanNode;
class Scheduler;

class ExecutionPlan final {
public:
    explicit ExecutionPlan(QueryContext* qctx);

    ~ExecutionPlan();

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    /**
     * Save all generated plan node in object pool.
     */
    PlanNode* addPlanNode(PlanNode* node);

    /**
     * Save all generated expression in object pool.
     */
    Expression* addExpression(Expression* expr);

    int64_t id() const {
        return id_;
    }

    const PlanNode* root() const {
        return root_;
    }

    folly::Future<Status> execute();

private:
    Executor* createExecutor();

    int64_t                                 id_;
    PlanNode*                               root_{nullptr};
    QueryContext*                           qctx_{nullptr};
    std::unique_ptr<IdGenerator>            nodeIdGen_;
    std::unique_ptr<Scheduler>              scheduler_;
};

}  // namespace graph
}  // namespace nebula

#endif
