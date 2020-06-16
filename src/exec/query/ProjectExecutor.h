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
    ProjectExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ProjectExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_PROJECTEXECUTOR_H_
