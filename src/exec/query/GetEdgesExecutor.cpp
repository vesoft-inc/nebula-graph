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
using nebula::storage::cpp2::EdgePropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        watch_.start();
        return getEdges().ensure([this]() { watch_.stop(); });
    }));
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    dumpLog();

    GraphStorageClient *client = ectx()->getStorageClient();
    if (client == nullptr) {
        return error(Status::Error("Invalid storage client for GetEdgesExecutor"));
    }

    auto *ge = asNode<GetEdges>(node());

    return client->getEdgeProps(ge->space(), ge->edges(), ge->props(), ge->filter())
        .via(runner())
        .then([this](StorageRpcResponse<EdgePropResponse> resp) {
            resp.responses();
            nebula::Value value;
            return finish(std::move(value));
        });
}

}   // namespace graph
}   // namespace nebula
