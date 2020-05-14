/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_LOOPEXECUTOR_H_
#define EXEC_QUERY_LOOPEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class LoopExecutor final : public Executor {
public:
<<<<<<< HEAD:src/exec/logic/LoopExecutor.h
    LoopExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *body);
=======
    LoopExecutor(const PlanNode *node, QueryContext *qctx, Executor *input, Executor *body)
        : SingleInputExecutor("LoopExecutor", node, qctx, input),
          body_(body) {}

    Status prepare() override;
>>>>>>> Replace ExecutionContext with QueryContext.:src/exec/query/LoopExecutor.h

    folly::Future<Status> execute() override;

    Executor *loopBody() const {
        return body_;
    }

private:
    // Hold the last executor node of loop body executors chain
    Executor *body_{nullptr};

    // Represent loop index. It will be updated and stored in QueryContext before starting loop
    // body. The mainly usage is that MultiOutputsExecutor could check whether current execution is
    // called multiple times.
    int64_t iterCount_{-1};
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_LOOPEXECUTOR_H_
