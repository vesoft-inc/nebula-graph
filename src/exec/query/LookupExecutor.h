/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef EXEC_QUERY_LOOKUPEXECUTOR_H_
#define EXEC_QUERY_LOOKUPEXECUTOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "exec/Executor.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class LookupExecutor final : public Executor {
public:
    LookupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("LookupExecutor", node, qctx) {
        gn_ = asNode<Lookup>(node);
    }

    Status handleResp(storage::StorageRpcResponse<storage::cpp2::LookupIndexResp> &&rpcResp) {
        auto completeness = handleCompleteness(rpcResp);
        if (!completeness.ok()) {
            return std::move(completeness).status();
        }
        auto state = std::move(completeness).value();
        nebula::DataSet v;
        for (auto &resp : rpcResp.responses()) {
            checkResponseResult(resp.get_result());
            if (resp.__isset.data) {
                nebula::DataSet* data = resp.get_data();
                // TODO : convert the column name to alias.
                if (v.colNames.size() == 0) {
                    v.colNames = data->colNames;
                }
                v.rows.insert(v.rows.end(),data->rows.begin(), data->rows.end());
            } else {
                state = Result::State::kPartialSuccess;
            }
        }
        return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kSequential)
                      .state(state)
                      .finish());
    }

    template <typename Resp>
    StatusOr<Result::State>
    handleCompleteness(const storage::StorageRpcResponse<Resp> &rpcResp) const {
        auto completeness = rpcResp.completeness();
        if (completeness != 100) {
            const auto &failedCodes = rpcResp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << name_ << " failed, error "
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second) << ", part "
                           << it->first;
            }
            if (completeness == 0) {
                return Status::Error("Lookup from storage failed in executor.");
            }
            return Result::State::kPartialSuccess;
        }
        return Result::State::kSuccess;
    }

    void checkResponseResult(const storage::cpp2::ResponseCommon& result) const {
        auto failedParts = result.get_failed_parts();
        if (!failedParts.empty()) {
            std::stringstream ss;
            for (auto& part : failedParts) {
                ss << "error code: " << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(part.get_code())
                   << ", leader: " << part.get_leader()->host << ":" << part.get_leader()->port
                   << ", part id: " << part.get_part_id() << "; ";
            }
            LOG(ERROR) << ss.str();
        }
    }
private:
    folly::Future<Status> execute() override;

    folly::Future<Status> lookup();

private:
    const Lookup*   gn_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_LOOKUPEXECUTOR_H_
