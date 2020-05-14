/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_PROJECTEXECUTOR_H_
#define EXEC_QUERY_PROJECTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class ProjectExecutor final : public Executor {
public:
<<<<<<< HEAD
    ProjectExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("ProjectExecutor", node, ectx) {}
=======
    ProjectExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("ProjectExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

private:
    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_PROJECTEXECUTOR_H_
