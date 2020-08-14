/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include <cstdint>
#include <memory>

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

namespace cpp2 {
class PlanDescription;
}  // namespace cpp2

class Executor;
class IdGenerator;
class PlanNode;
class Scheduler;

class ExecutionPlan final {
public:
    explicit ExecutionPlan(ObjectPool* objectPool);

    ~ExecutionPlan();

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    /**
     * Save all generated plan node in object pool.
     */
    PlanNode* addPlanNode(PlanNode* node);

    /**
     * Save all generated object in object pool.
     */
    template <typename T>
    T* saveObject(T* obj) {
        return objPool_->add(obj);
    }

    // follow the flavor like std::make_unique, combine the object creation and ownership holding
    template <typename T, typename... Args>
    T* makeAndSave(Args&&... args) {
        return objPool_->makeAndAdd<T>(std::forward<Args>(args)...);
    }

    int64_t id() const {
        return id_;
    }

    PlanNode* root() const {
        return root_;
    }

    ObjectPool* objPool() const {
        return objPool_;
    }

    void fillPlanDescription(cpp2::PlanDescription* planDesc) const;

private:
    Executor* createExecutor();

    int64_t                                 id_;
    PlanNode*                               root_{nullptr};
    ObjectPool*                             objPool_{nullptr};
    std::unique_ptr<IdGenerator>            nodeIdGen_;
};

}  // namespace graph
}  // namespace nebula

#endif
