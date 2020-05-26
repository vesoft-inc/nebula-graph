/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_UPDATEEXECUTOR_H_
#define EXEC_MUTATE_UPDATEEXECUTOR_H_

#include "common/base/StatusOr.h"
#include "exec/Executor.h"

namespace nebula {
namespace graph {

class UpdateBaseExecutor : public SingleInputExecutor {
public:
    UpdateBaseExecutor(const std::string &execName,
                       const PlanNode *node,
                       ExecutionContext *ectx)
            : SingleInputExecutor(execName, node, ectx) {}

    virtual ~UpdateBaseExecutor() {}

protected:
    StatusOr<DataSet> handleResult(const DataSet &data);

protected:
    std::vector<std::string>         yieldPros_;
};

class UpdateVertexExecutor final : public UpdateBaseExecutor {
public:
    UpdateVertexExecutor(const PlanNode *node, ExecutionContext *ectx)
        : UpdateBaseExecutor("UpdateVertexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> updateVertex();
};

class UpdateEdgeExecutor final : public UpdateBaseExecutor {
public:
    UpdateEdgeExecutor(const PlanNode *node, ExecutionContext *ectx)
            : UpdateBaseExecutor("UpdateEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> updateEdge();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_UPDATEEXECUTOR_H_
