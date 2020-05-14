/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_FILTEREXECUTOR_H_
#define EXEC_QUERY_FILTEREXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class FilterExecutor final : public Executor {
public:
<<<<<<< HEAD
    FilterExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("FilterExecutor", node, ectx) {}
=======
    FilterExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("FilterExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_FILTEREXECUTOR_H_
