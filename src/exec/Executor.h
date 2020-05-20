/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTOR_H_
#define EXEC_EXECUTOR_H_

#include <list>
#include <memory>
#include <string>
#include <vector>

#include <folly/futures/Future.h>

#include "base/Status.h"
#include "cpp/helpers.h"
#include "datatypes/Value.h"
#include "planner/Query.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

class PlanNode;
class ExecutionContext;

class Executor : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // For check whether a task is a Executor::Callable by std::is_base_of<>::value in thread pool
    struct Callable {
        int64_t planId;

        explicit Callable(const Executor *e);
    };

    // Enable thread pool check the query plan id of each callback registered in future. The functor
    // is only the proxy of the invocable function fn.
    template <typename F>
    struct Callback : Callable {
        using Extract = folly::futures::detail::Extract<F>;
        using Return = typename Extract::Return;
        using FirstArg = typename Extract::FirstArg;

        F fn;

        Callback(const Executor *e, F f) : Callable(e), fn(std::move(f)) {}

        Return operator()(FirstArg &&arg) {
            return fn(std::forward<FirstArg>(arg));
        }
    };

    // Create executor according to plan node
    static Executor *makeExecutor(const PlanNode *node,
                                  ExecutionContext *ectx,
                                  std::unordered_map<int64_t, Executor *> *cache);

    virtual ~Executor() {}

    // Preparation before executing
    virtual Status prepare() {
        return Status::OK();
    }

    // Implementation interface of operation logic
    virtual folly::Future<Status> execute() = 0;

    ExecutionContext *ectx() const {
        return ectx_;
    }

    int64_t id() const;

    const std::string &name() const {
        return name_;
    }

    const PlanNode *node() const {
        return node_;
    }

    template <typename Fn>
    Callback<Fn> cb(Fn &&f) const {
        return Callback<Fn>(this, std::forward<Fn>(f));
    }

#define NODE_DOWN_CAST(node) \
    template <typename T> \
    static std::enable_if_t<std::is_same<node##Node, T>::value, const T *> \
    asNode(const PlanNode* from) { \
        CHECK_EQ(from->kind(), PlanNode::Kind::k##node); \
        return static_cast<const node##Node *>(from); \
    }

    NODE_DOWN_CAST(Start)
    NODE_DOWN_CAST(End)
    NODE_DOWN_CAST(GetNeighbors)
    NODE_DOWN_CAST(GetVertices)
    NODE_DOWN_CAST(GetEdges)
    NODE_DOWN_CAST(ReadIndex)
    NODE_DOWN_CAST(Filter)
    NODE_DOWN_CAST(Union)
    NODE_DOWN_CAST(Intersect)
    NODE_DOWN_CAST(Minus)
    NODE_DOWN_CAST(Project)
    NODE_DOWN_CAST(Sort)
    NODE_DOWN_CAST(Limit)
    NODE_DOWN_CAST(Aggregate)
    NODE_DOWN_CAST(Selector)
    NODE_DOWN_CAST(Loop)
    NODE_DOWN_CAST(SwitchSpace)
    NODE_DOWN_CAST(Dedup)
    NODE_DOWN_CAST(MultiOutputs)
    NODE_DOWN_CAST(CreateSpace)
    NODE_DOWN_CAST(CreateTag)
    NODE_DOWN_CAST(CreateEdge)
    NODE_DOWN_CAST(DescSpace)
    NODE_DOWN_CAST(DescTag)
    NODE_DOWN_CAST(DescEdge)
    NODE_DOWN_CAST(InsertVertices)
    NODE_DOWN_CAST(InsertEdges)

#undef NODE_DOWN_CAST


protected:
    // Only allow derived executor to construct
    Executor(const std::string &name, const PlanNode *node, ExecutionContext *ectx);

    // Start a future chain and bind it to thread pool
    folly::Future<Status> start(Status status = Status::OK()) const;

    // Throw runtime error to stop whole execution early
    folly::Future<Status> error(Status status) const;

    folly::Executor *runner() const;

    // Store the result of this executor to execution context
    Status finish(nebula::Value &&value);

    // Dump some execution logging messages
    void dumpLog() const;

    // Executor name
    std::string name_;

    // Relative Plan Node
    const PlanNode *node_;

    // Execution context for saving some execution data
    ExecutionContext *ectx_;

    // TODO: Some statistics
};

class SingleInputExecutor : public Executor {
public:
    Status prepare() override {
        return input_->prepare();
    }

    folly::Future<Status> execute() override;

protected:
    SingleInputExecutor(const std::string &name,
                        const PlanNode *node,
                        ExecutionContext *ectx,
                        Executor *input)
        : Executor(name, node, ectx), input_(input) {
        DCHECK_NOTNULL(input);
    }

    Executor *input_;
};

class MultiInputsExecutor : public Executor {
public:
    Status prepare() override {
        for (auto input : inputs_) {
            auto status = input->prepare();
            if (!status.ok()) return status;
        }
        return Status::OK();
    }

    folly::Future<Status> execute() override;

protected:
    MultiInputsExecutor(const std::string &name,
                        const PlanNode *node,
                        ExecutionContext *ectx,
                        std::vector<Executor *> &&inputs)
        : Executor(name, node, ectx), inputs_(std::move(inputs)) {
        DCHECK(!inputs_.empty());
    }

    std::vector<Executor *> inputs_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTOR_H_
