/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _EXEC_STORAGE_EXECUTOR_H_
#define _EXEC_STORAGE_EXECUTOR_H_

#include "exec/Executor.h"
#include "common/clients/storage/StorageClientBase.h"

namespace nebula {
namespace graph {

// It's used for data write/update/query
class StorageExecutor : public Executor {
protected:
    StorageExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

    // parameter isCompleteRequire to specify is return error when partial succeeded
    template <typename Resp>
    StatusOr<Result::State>
    handleCompleteness(const storage::StorageRpcResponse<Resp> &rpcResp,
                       bool isCompleteRequire) const {
        auto completeness = rpcResp.completeness();
        if (completeness != 100) {
            const auto &failedCodes = rpcResp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << name_ << " failed, error "
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second) << ", part "
                           << it->first;
            }
            if (completeness == 0 || isCompleteRequire) {
                LOG(ERROR) << "Request to storage failed in executor `" << name_ << "'";
                return Status::Error("Request to storage failed in executor.");
            }
            return Result::State::kPartialSuccess;
        }
        return Result::State::kSuccess;
    }
};

}   // namespace graph
}   // namespace nebula

#endif  // _EXEC_STORAGE_EXECUTOR_H_
