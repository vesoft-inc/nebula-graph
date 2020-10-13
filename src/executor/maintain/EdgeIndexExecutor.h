/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MAINTAIN_EDGEINDEXEXECUTOR_H_
#define EXECUTOR_MAINTAIN_EDGEINDEXEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeIndexExecutor final : public Executor {
public:
    CreateEdgeIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("CreateEdgeIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropEdgeIndexExecutor final : public Executor {
public:
    DropEdgeIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropEdgeIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DescEdgeIndexExecutor final : public Executor {
public:
    DescEdgeIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescEdgeIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class RebuildEdgeIndexExecutor final : public Executor {
public:
    RebuildEdgeIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("RebuildEdgeIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MAINTAIN_EDGEINDEXEXECUTOR_H_
