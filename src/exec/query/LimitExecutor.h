/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_LIMITEXECUTOR_H_
#define EXEC_QUERY_LIMITEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class LimitExecutor final : public Executor {
public:
<<<<<<< HEAD
    LimitExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("LimitExecutor", node, ectx) {}
=======
    LimitExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("LimitExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

private:
    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_LIMITEXECUTOR_H_
