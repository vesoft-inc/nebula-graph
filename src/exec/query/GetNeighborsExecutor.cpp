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

#include "planner/Query.h"
#include "context/QueryContext.h"


namespace nebula {
namespace graph {

folly::Future<Status> GetNeighborsExecutor::execute() {
    return getNeighbors().ensure([this]() {
        // TODO(yee): some cleanup or stats actions
        UNUSED(this);
    });
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    const GetNeighbors* gn = asNode<GetNeighbors>(node());
<<<<<<< HEAD
    std::vector<std::string> colNames;
=======
    Expression* srcExpr = gn->src();
    Value value = srcExpr->eval();
    DCHECK_EQ(value.type(), Value::Type::DATASET);
    auto& input = value.getDataSet();
>>>>>>> Implement get neighbors.

    GraphStorageClient* storageClient = qctx_->getStorageClient();
    return storageClient
        ->getNeighbors(gn->space(),
                       std::move(input.colNames),
                       std::move(input.rows),
                       gn->edgeTypes(),
                       gn->edgeDirection(),
                       &gn->statProps(),
                       nullptr,   // FIXME
                       nullptr,
                       gn->dedup(),
                       gn->orderBy(),
                       gn->limit(),
                       gn->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            auto status = handleResponse(resp);
            return status.ok() ? start() : error(std::move(status));
        });
}

Status GetNeighborsExecutor::handleResponse(StorageRpcResponse<GetNeighborsResponse>& resps) {
    auto completeness = resps.completeness();
    if (completeness != 0) {
        return Status::Error("Get neighbors failed");
    }

    State state;
    if (completeness != 100) {
        state = State(State::Stat::kPartialSuccess,
                    folly::stringPrintf("Get neighbors partially failed: %d %%", completeness));
    }

    auto& responses = resps.responses();
    List list;
    for (auto& resp : responses) {
        checkResponseResult(resp.get_result());

        auto dataset = resp.get_vertices();
        if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
        }

        list.values.emplace_back(std::move(*dataset));
    }
    return finish(Result::buildGetNeighbors(Value(std::move(list)), std::move(state)));
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
