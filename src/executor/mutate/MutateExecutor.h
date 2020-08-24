/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MUTATE_MUTATEEXECUTOR_H_
#define EXECUTOR_MUTATE_MUTATEEXECUTOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class MutateExecutor : public Executor {
public:
    MutateExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

protected:
    using RpcResponse = nebula::storage::StorageRpcResponse<nebula::storage::cpp2::ExecResponse>;
    GraphStatus handleResponse(const RpcResponse &resp, const std::string &executorName) {
        auto completeness = resp.completeness();
        Status status;
        if (completeness != 100) {
            const auto& failedCodes = resp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << executorName << " failed, error "
                           << nebula::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                           << ", part " << it->first;
            }
            return GraphStatus::setRpcResponse(failedCodes.begin()->second, "");
        }
        return GraphStatus::OK();
    }
};
}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MUTATE_MUTATEEXECUTOR_H_
