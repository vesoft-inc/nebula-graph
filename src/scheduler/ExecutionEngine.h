/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_EXECUTIONENGINE_H_
#define SCHEDULER_EXECUTIONENGINE_H_

#include <memory>

namespace nebula {

class ObjectPool;

namespace graph {

class PlanNode;
class ExecutionContext;
class Executor;

class ExecutionEngine final {
public:
    explicit ExecutionEngine(std::shared_ptr<PlanNode> planRoot);

    Executor *makeExecutor() const;

private:
    std::shared_ptr<PlanNode> planRoot_;
    std::unique_ptr<ObjectPool> objPool_;
    std::unique_ptr<ExecutionContext> ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_EXECUTIONENGINE_H_
