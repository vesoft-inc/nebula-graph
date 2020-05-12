/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetEdgesExecutor.h"

// common
#include "clients/storage/GraphStorageClient.h"
// graph
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        return getEdges().ensure([this]() {
            // TODO(yee): some cleanup or stats actions
            UNUSED(this);
        });
    }));
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    CHECK_NODE_TYPE(GetEdges);
    dumpLog();

    GraphStorageClient *client = ectx()->getStorageClient();
    if (client == nullptr) {
        return error(Status::Error("Invalid storage client for GetEdgesExecutor"));
    }

    auto *ge = asNode<GetEdges>(node());
    std::vector<std::string> cols{nebula::_SRC, nebula::_TYPE, nebula::_RANK, nebula::_DST};
    for (const auto &prop : ge->props()) {
        cols.emplace_back(prop);
    }
    return client
        ->getProps(ge->space(),
                   cols,
                   ge->edges(),
                   ge->props(),
                   ge->dedup(),
                   ge->orderBy(),
                   ge->limit(),
                   ge->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetPropResponse> rpcResp) {
            HANDLE_COMPLETENESS(rpcResp);
            // Ok, merge DataSets to one
            nebula::DataSet v;
            for (auto &resp : rpcResp.responses()) {
                auto *props = resp.get_props();
                if (props != nullptr) {
                    v.merge(std::move(*props));
                }
            }
            finish(std::move(v));
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
