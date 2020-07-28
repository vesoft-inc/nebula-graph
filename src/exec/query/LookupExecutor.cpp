/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LookupExecutor.h"
#include "planner/PlanNode.h"
#include "context/QueryContext.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::LookupIndexResp;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> LookupExecutor::execute() {
    return lookup();
}

folly::Future<Status> LookupExecutor::lookup() {
    dumpLog();
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    auto *lookup = asNode<Lookup>(node());
    return storageClient->lookupIndex(lookup->space(),
                                      *lookup->queryContext(),
                                      lookup->isEdge(),
                                      lookup->schemaId(),
                                      *lookup->returnColumns())
        .via(runner())
        .then([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
            return handleResp(std::move(rpcResp));
        });
}

}   // namespace graph
}   // namespace nebula
