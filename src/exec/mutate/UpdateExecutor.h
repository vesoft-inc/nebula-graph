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

class UpdateBaseExecutor : public Executor {
public:
    UpdateBaseExecutor(const std::string &execName,
                       const PlanNode *node,
                       QueryContext *ectx)
        : Executor(execName, node, ectx) {}

    virtual ~UpdateBaseExecutor() {}

protected:
    StatusOr<DataSet> handleResult(const DataSet &data);

    Status handleErrorCode(nebula::storage::cpp2::ErrorCode code, PartitionID partId);

protected:
    std::vector<std::string>         yieldNames_;
    std::string                      schemaName_;
};

class UpdateVertexExecutor final : public UpdateBaseExecutor {
public:
    UpdateVertexExecutor(const PlanNode *node, QueryContext *ectx)
        : UpdateBaseExecutor("UpdateVertexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class UpdateEdgeExecutor final : public UpdateBaseExecutor {
public:
    UpdateEdgeExecutor(const PlanNode *node, QueryContext *ectx)
        : UpdateBaseExecutor("UpdateEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_UPDATEEXECUTOR_H_
