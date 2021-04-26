/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_STORAGEACCESSEXECUTOR_H_
#define EXECUTOR_STORAGEACCESSEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "common/clients/storage/StorageClientBase.h"
#include "common/interface/gen-cpp2/common_constants.h"
#include "context/QueryContext.h"
#include "executor/Executor.h"
#include "service/Session.h"

namespace nebula {

class Expression;

namespace graph {

class Iterator;
struct SpaceInfo;

// It's used for data write/update/query
class StorageAccessExecutor : public Executor {
protected:
    StorageAccessExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

    // Parameter isPartialSuccessAccepted to specify
    // whether to return an error for partial succeeded.
    // An error will be returned if isPartialSuccessAccepted
    // is set to false and completeness is less than 100.
    template <typename Resp>
    StatusOr<Result::State>
    handleCompleteness(const storage::StorageRpcResponse<Resp> &rpcResp,
                       bool isPartialSuccessAccepted) const {
        auto completeness = rpcResp.completeness();
        if (completeness != 100) {
            const auto &failedCodes = rpcResp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << name_ << " failed, error "
                           << apache::thrift::util::enumNameSafe(it->second) << ", part "
                           << it->first;
            }
            // cannot execute at all, or partial success is not accepted
            if (completeness == 0 || !isPartialSuccessAccepted) {
                if (failedCodes.size() > 0) {
                    return handleErrorCode(failedCodes.begin()->second,
                                           failedCodes.begin()->first);
                }
                return Status::Error("Request to storage failed, without failedCodes.");
            }
            // partial success is accepted
            qctx()->setPartialSuccess();
            return Result::State::kPartialSuccess;
        }
        return Result::State::kSuccess;
    }

    Status handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const {
        qctx_->rctx()->resp().errorCode = static_cast<nebula::ErrorCode>(code);
        if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
            return Status::OK();
        }
        auto &errorMsgMap = nebula::cpp2::common_constants::ErrorMsgUTF8Map();
        auto findIter = errorMsgMap.find(code);
        if (findIter == errorMsgMap.end()) {
            auto status = Status::Error("Storage Error: part: %d, error: %s(%d).",
                                        partId,
                                        apache::thrift::util::enumNameSafe(code).c_str(),
                                        static_cast<int32_t>(code));
            LOG(ERROR) << status;
            return status;
        }

        auto resultIter = findIter->second.find(nebula::cpp2::Language::L_EN);
        if (resultIter != findIter->second.end()) {
            return Status::Error(resultIter->second);
        }
        return Status::Error("Unknown language L_EN");
    }

    template<typename RESP>
    void addStats(RESP& resp, std::unordered_map<std::string, std::string>& stats) const {
        auto& hostLatency = resp.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
            auto& info = hostLatency[i];
            stats.emplace(
                folly::stringPrintf("%s exec/total", std::get<0>(info).toString().c_str()),
                folly::stringPrintf("%d(us)/%d(us)", std::get<1>(info), std::get<2>(info)));
        }
    }

    bool isIntVidType(const SpaceInfo &space) const;

    DataSet buildRequestDataSetByVidType(Iterator *iter, Expression *expr, bool dedup);
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_STORAGEACCESSEXECUTOR_H_
