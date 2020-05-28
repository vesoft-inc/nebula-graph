/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_GETEDGESEXECUTOR_H_
#define EXEC_QUERY_GETEDGESEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GetEdgesExecutor final : public Executor {
public:
<<<<<<< HEAD
    GetEdgesExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("GetEdgesExecutor", node, ectx) {}
=======
    GetEdgesExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("GetEdgesExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getEdges();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETEDGESEXECUTOR_H_
