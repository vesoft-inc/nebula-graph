/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetEdgesExecutor.h"

<<<<<<< HEAD
// common
#include "common/clients/storage/GraphStorageClient.h"
// graph
=======
#include "clients/storage/GraphStorageClient.h"
>>>>>>> Replace ExecutionContext with QueryContext.
#include "planner/Query.h"
#include "context/QueryContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    return getEdges();
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    dumpLog();

    GraphStorageClient *client = qctx()->getStorageClient();
    if (client == nullptr) {
        return error(Status::Error("Invalid storage client for GetEdgesExecutor"));
    }

    // auto *ge = asNode<GetEdges>(node());

    // return client->getProps(ge->space(), ge->edges(), ge->props(), ge->filter())
    //     .via(runner())
    //     .then([this](StorageRpcResponse<GetPropResponse> resp) {
    //         resp.responses();
    //         nebula::Value value;
    //         return finish(std::move(value));
    //     });
    return start();
}

}   // namespace graph
}   // namespace nebula
