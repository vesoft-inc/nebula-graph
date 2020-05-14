/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_SELECTEXECUTOR_H_
#define EXEC_QUERY_SELECTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class SelectExecutor final : public Executor {
public:
<<<<<<< HEAD:src/exec/logic/SelectExecutor.h
    SelectExecutor(const PlanNode* node, ExecutionContext* ectx, Executor* then, Executor* els);
=======
    SelectExecutor(const PlanNode* node,
                   QueryContext* qctx,
                   Executor* input,
                   Executor* then,
                   Executor* els);

    Status prepare() override;
>>>>>>> Replace ExecutionContext with QueryContext.:src/exec/query/SelectExecutor.h

    folly::Future<Status> execute() override;

    Executor* thenBody() const {
        return then_;
    }

    Executor* elseBody() const {
        return else_;
    }

private:
    Executor* then_;
    Executor* else_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_SELECTEXECUTOR_H_
