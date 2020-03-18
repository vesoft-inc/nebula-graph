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
#include "interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

class PlanNode;
class ExecutionContext;

class Executor : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // Create executor according to plan node
    static Executor *makeExecutor(const PlanNode &node, ExecutionContext *ectx);

    virtual ~Executor() {}

    // Preparation before executing
    virtual Status prepare() {
        return Status::OK();
    }

    // Implementation interface of operation logic
    virtual folly::Future<Status> execute() = 0;

    const ExecutionContext *ectx() const {
        return ectx_;
    }

    uint64_t id() const {
        return id_;
    }

    const std::string &name() const {
        return name_;
    }

    const PlanNode *node() const {
        return node_;
    }

protected:
    // Only allow derived executor to construct
    Executor(const std::string &name, const PlanNode *node, ExecutionContext *ectx)
        : name_(name), node_(node), ectx_(ectx) {
        DCHECK_NOTNULL(node_);
        DCHECK_NOTNULL(ectx_);
    }

    // Store the result of this executor to execution context
    Status finish(std::list<cpp2::Row> dataset);

    // Executor unique id
    uint64_t id_;

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
    folly::Future<Status> execute() override {
        folly::Future<Status> fut = input_->execute();
        return std::move(fut).then([this](Status s) {
            if (!s.ok()) return folly::makeFuture(s);
            return this->exec();
        });
    }

protected:
    SingleInputExecutor(const std::string &name,
                        const PlanNode *node,
                        ExecutionContext *ectx,
                        Executor *input)
        : Executor(name, node, ectx), input_(input) {
        DCHECK_NOTNULL(input);
    }

    virtual folly::Future<Status> exec() {
        return Status::OK();
    }

    Executor *input_;
};

class MultiInputsExecutor : public Executor {
public:
    folly::Future<Status> execute() override {
        std::vector<folly::Future<Status>> futures;
        for (auto *in : inputs_) {
            futures.push_back(in->execute());
        }
        return folly::collect(futures).then([this](std::vector<Status> ss) {
            for (auto &s : ss) {
                if (!s.ok()) return folly::makeFuture(s);
            }
            return this->exec();
        });
    }

protected:
    MultiInputsExecutor(const std::string &name,
                        const PlanNode *node,
                        ExecutionContext *ectx,
                        std::vector<Executor *> &&inputs)
        : Executor(name, node, ectx), inputs_(std::move(inputs)) {
        DCHECK(!inputs_.empty());
    }

    virtual folly::Future<Status> exec() {
        return Status::OK();
    }

    std::vector<Executor *> inputs_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTOR_H_
