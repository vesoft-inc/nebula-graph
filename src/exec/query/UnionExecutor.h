/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_UNIONEXECUTOR_H_
#define EXEC_QUERY_UNIONEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class UnionExecutor : public Executor {
public:
<<<<<<< HEAD
    UnionExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("UnionExecutor", node, ectx) {}
=======
    UnionExecutor(const PlanNode *node, QueryContext *qctx, Executor *left, Executor *right);
>>>>>>> Replace ExecutionContext with QueryContext.

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_UNIONEXECUTOR_H_
