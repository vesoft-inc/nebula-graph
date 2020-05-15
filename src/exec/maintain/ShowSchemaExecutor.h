/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXEC_MAINTAIN_SHOWSCHEMAEXECUTOR_H_
#define EXEC_MAINTAIN_SHOWSCHEMAEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class ShowTagsExecutor final : public Executor {
public:
    ShowTagsExecutor(const PlanNode *node, ExecutionContext *ectx)
            : Executor("ShowTagsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowEdgesExecutor final : public Executor {
public:
    ShowEdgesExecutor(const PlanNode *node, ExecutionContext *ectx)
            : Executor("ShowEdgesExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_SHOWSCHEMAEXECUTOR_H_
