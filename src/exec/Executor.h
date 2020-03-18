/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTOR_H_
#define EXEC_EXECUTOR_H_

#include <list>
#include <memory>
#include <vector>

#include <folly/futures/Future.h>

#include "base/Status.h"
#include "interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

class PlanNode;
class ExecutionContext;

class Executor {
public:
    // Create executor according to plan node
    static std::shared_ptr<Executor> makeExecutor(const PlanNode &node, ExecutionContext *ectx);

    // Implementation interface of operation logic
    virtual folly::Future<void> execute() = 0;

    ExecutionContext *ectx() const {
        return ectx_;
    }

    uint64_t id() const {
        return id_;
    }

    PlanNode *node() const {
        return node_;
    }

    void dependsOn(Executor *dep) {
        depends_.push_back(dep);
    }

protected:
    // Only allow derived executor to construct
    explicit Executor(PlanNode *node, ExecutionContext *ectx) : node_(node), ectx_(ectx) {}

    // Store the result of this executor to execution context
    Status finish(std::list<cpp2::Row> dataset);

    // Executor unique id
    uint64_t id_;

    // Relative Plan Node
    PlanNode *node_{nullptr};

    // Execution context for saving some execution data
    ExecutionContext *ectx_{nullptr};

    // TODO: Some statistics

    // dependencies
    std::vector<Executor *> depends_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTOR_H_
