/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetNeighborsExecutor.h"

#include <sstream>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"

#include "context/QueryContext.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> GetNeighborsExecutor::execute() {
    dumpLog();
    auto status = buildRequestDataSet();
    if (!status.ok()) {
        return error(std::move(status));
    }
    return getNeighbors().ensure([this]() {
        // TODO(yee): some cleanup or stats actions
        UNUSED(this);
    });
}

Status GetNeighborsExecutor::buildRequestDataSet() {
    // clear the members
    reqDs_.rows.clear();

    auto& inputVar = gn_->inputVar();
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    ExpressionContextImpl ctx(ectx_, iter.get());
    DataSet input;
    reqDs_.colNames = {"_vid"};
    reqDs_.rows.reserve(iter->size());
    auto* src = gn_->src();
    for (; iter->valid(); iter->next()) {
        auto val = Expression::eval(src, ctx);
        if (!val.isStr()) {
            continue;
        }
        Row row;
        row.emplace_back(std::move(val));
        reqDs_.rows.emplace_back(std::move(row));
    }

    return Status::OK();
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    if (reqDs_.rows.empty()) {
        LOG(INFO) << "Empty input.";
        return folly::makeFuture(Status::OK());
    }
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    return storageClient
        ->getNeighbors(gn_->space(),
                       std::move(reqDs_.colNames),
                       std::move(reqDs_.rows),
                       gn_->edgeTypes(),
                       gn_->edgeDirection(),
                       gn_->statProps(),
                       gn_->vertexProps(),
                       gn_->edgeProps(),
                       gn_->exprs(),
                       gn_->dedup(),
                       gn_->random(),
                       gn_->orderBy(),
                       gn_->limit(),
                       gn_->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            auto status = handleResponse(resp);
            return status.ok() ? start() : error(std::move(status));
        });
}

Status GetNeighborsExecutor::handleResponse(RpcResponse& resps) {
    auto completeness = resps.completeness();
    if (completeness == 0) {
        return Status::Error("Get neighbors failed");
    }

    State state(State::Stat::kSuccess, "");
    if (completeness != 100) {
        state = State(State::Stat::kPartialSuccess,
                    folly::stringPrintf("Get neighbors partially failed: %d %%", completeness));
    }

    auto& responses = resps.responses();
    VLOG(1) << "Resp size: " << responses.size();
    List list;
    for (auto& resp : responses) {
        checkResponseResult(resp.get_result());

        auto dataset = resp.get_vertices();
        if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
        }

        VLOG(1) << "Resp row size: " << dataset->rows.size();
        list.values.emplace_back(std::move(*dataset));
    }
    auto result = Value(std::move(list));
    return finish(ExecResult::buildGetNeighbors(std::move(result), std::move(state)));
}

void GetNeighborsExecutor::checkResponseResult(const storage::cpp2::ResponseCommon& result) const {
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

}   // namespace graph
}   // namespace nebula
