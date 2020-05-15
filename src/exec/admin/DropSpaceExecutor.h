/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXEC_ADMIN_DROPSPACEEXECUTOR_H_
#define EXEC_ADMIN_DROPSPACEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class DropSpaceExecutor final : public Executor {
public:
    DropSpaceExecutor(const PlanNode *node, ExecutionContext *ectx)
            : Executor("DropSpaceExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_DROPSPACEEXECUTOR_H_
