/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MULTIOUTPUTSEXECUTOR_H
#define EXEC_MULTIOUTPUTSEXECUTOR_H

#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/SharedPromise.h>

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class MultiOutputsExecutor final : public SingleInputExecutor {
public:
    MultiOutputsExecutor(const PlanNode *node,
                         ExecutionContext *ectx,
                         Executor *input,
                         bool isInLoopBody,
                         const std::string &iterVarName)
        : SingleInputExecutor("MultiOutputsExecutor", node, ectx, input),
          isInLoopBody_(isInLoopBody),
          iterVarName_(iterVarName) {
        DCHECK(!inInLoopBody_ || !iterVarName_.empty());
    }

    folly::Future<Status> execute() override;

private:
    using PromisePtr = std::shared_ptr<folly::SharedPromise<Status>>;

    folly::SpinLock lock_;

    // This shared promise to notify all other output executors to run except the guy calling this
    // executor firstly
    std::unordered_map<int64_t, PromisePtr> promiseMap_;

    bool hasBeenRun_{false};
    bool isInLoopBody_{false};
    int64_t iterCount_{-1};
    std::string iterVarName_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MULTIOUTPUTSEXECUTOR_H
