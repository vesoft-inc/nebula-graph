/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_SORTEXECUTOR_H_
#define EXEC_QUERY_SORTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class SortExecutor final : public Executor {
public:
<<<<<<< HEAD
    SortExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("SortExecutor", node, ectx) {}
=======
    SortExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("SortExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_SORTEXECUTOR_H_
