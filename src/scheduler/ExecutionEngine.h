/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_EXECUTIONENGINE_H_
#define SCHEDULER_EXECUTIONENGINE_H_

#include "cpp/helpers.h"

namespace nebula {

class ObjectPool;

namespace graph {

class PlanNode;
class ExecutionContext;
class Executor;

class ExecutionEngine final : private cpp::NonMovable, private cpp::NonCopyable {
public:
    explicit ExecutionEngine(const PlanNode* planRoot);

    ~ExecutionEngine();

    Executor* makeExecutor();

private:
    const PlanNode* planRoot_;
    ObjectPool* objPool_;
    ExecutionContext* ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_EXECUTIONENGINE_H_
