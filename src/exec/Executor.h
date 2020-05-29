/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTOR_H_
#define EXEC_EXECUTOR_H_

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"
#include "common/datatypes/Value.h"
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
    // Create executor according to plan node
    static Executor *makeExecutor(const PlanNode *node,
                                  ExecutionContext *ectx,
                                  std::unordered_map<int64_t, Executor *> *cache);

    virtual ~Executor() {}

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

    const std::set<Executor *> &depends() const {
        return depends_;
    }

    const std::set<Executor *> &successors() const {
        return successors_;
    }

    Executor *addDependent(Executor *dep) {
        depends_.emplace(dep);
        dep->successors_.emplace(this);
        return this;
    }

    template <typename T>
    static std::enable_if_t<std::is_base_of<PlanNode, T>::value, const T *> asNode(
        const PlanNode *node) {
        return static_cast<const T *>(node);
    }

    // Throw runtime error to stop whole execution early
    folly::Future<Status> error(Status status) const;

protected:
    // Only allow derived executor to construct
    Executor(const std::string &name, const PlanNode *node, ExecutionContext *ectx);

    // Start a future chain and bind it to thread pool
    folly::Future<Status> start(Status status = Status::OK()) const;

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

    // Topology
    std::set<Executor *> depends_;
    std::set<Executor *> successors_;

    // TODO: Some statistics
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTOR_H_
