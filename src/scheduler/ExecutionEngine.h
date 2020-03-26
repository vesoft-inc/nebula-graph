/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_EXECUTIONENGINE_H_
#define SCHEDULER_EXECUTIONENGINE_H_

#include <memory>

#include "cpp/helpers.h"

namespace nebula {

class ObjectPool;

namespace graph {

class PlanNode;
class ExecutionContext;
class Executor;

class ExecutionEngine final : private cpp::NonMovable, private cpp::NonCopyable {
public:
    explicit ExecutionEngine(std::shared_ptr<PlanNode> planRoot);

    ~ExecutionEngine() = default;

    Executor *makeExecutor() const;

private:
    std::shared_ptr<PlanNode> planRoot_;
    std::shared_ptr<ObjectPool> objPool_;
    std::shared_ptr<ExecutionContext> ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_EXECUTIONENGINE_H_
