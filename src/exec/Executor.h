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
#include "service/GraphFlags.h"

#define CHECK_NODE_TYPE(kKind)                                                                     \
    do {                                                                                           \
        CHECK_EQ(node()->kind(), PlanNode::Kind::k##kKind);                                        \
    } while (0);

// pass the StorageRPCResponse struct instead of completeness number to avoid user pass a not
// related interger
#define HANDLE_COMPLETENESS(rpcResp)                                                               \
    do {                                                                                           \
        auto completeness = rpcResp.completeness();                                                \
        if (completeness != 100) {                                                                 \
            const auto &failedCodes = rpcResp.failedParts();                                       \
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {                   \
                LOG(ERROR) << __FUNCTION__ << " failed, error "                                    \
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)             \
                           << ", part " << it->first;                                              \
            }                                                                                      \
            if (FLAGS_strict_responses_check) {                                                    \
                return Status::Error("%s complete, completeness: %d", __FUNCTION__, completeness); \
            } else if (completeness == 0) {                                                        \
                return Status::Error(                                                              \
                    "%s not complete, completeness: %d", __FUNCTION__, completeness);              \
            }                                                                                      \
        }                                                                                          \
    } while (0);

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

    template <typename T>
    static std::enable_if_t<std::is_base_of<PlanNode, T>::value, const T *> asNode(
        const PlanNode *node) {
        return static_cast<const T *>(node);
    }

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
