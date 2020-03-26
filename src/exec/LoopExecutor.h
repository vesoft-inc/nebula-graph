/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_LOOPEXECUTOR_H_
#define EXEC_LOOPEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class LoopExecutor final : public SingleInputExecutor {
public:
    LoopExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input, Executor *body)
        : SingleInputExecutor("LoopExecutor", node, ectx, input), body_(body) {}

    folly::Future<Status> exec() override;

private:
    Executor *body_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_LOOPEXECUTOR_H_
