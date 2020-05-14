/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetVerticesExecutor.h"

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

folly::Future<Status> GetVerticesExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        return getVertices().ensure([this]() {
            // TODO(yee): some cleanup or stats actions
            UNUSED(this);
        });
    }));
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    CHECK_NODE_TYPE(GetVertices)
    dumpLog();

    auto *gv = asNode<GetVertices>(node());

    GraphStorageClient *storageClient = ectx()->getStorageClient();
    if (storageClient == nullptr) {
        return error(Status::Error("Invalid storage client for GetVerticesExecutor"));
    }
    nebula::DataSet vertices({nebula::_VID});
    if (!gv->vertices().empty()) {
        vertices.rows.insert(vertices.rows.end(),
                             std::make_move_iterator(gv->vertices().begin()),
                             std::make_move_iterator(gv->vertices().end()));
    }
    if (gv->src() != nullptr) {
        // TODO(shylock) pass expression context
        // Accept List[Str]
        auto src = gv->src()->eval();
        if (src.type() != Value::Type::LIST) {
            return error(Status::Error("Invalid vertex expression"));
        }
        for (std::size_t i = 0; i < src.getList().values.size(); ++i) {
            if (!src.getList().values[i].isStr()) {
                return Status::NotSupported("Invalid vid");
            }
            vertices.emplace_back(nebula::Row({std::move(src).getList().values[i].getStr()}));
        }
    }
    return storageClient
        ->getProps(gv->space(),
                   std::move(vertices),
                   gv->props(),
                   gv->dedup(),
                   gv->orderBy(),
                   gv->limit(),
                   gv->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetPropResponse> rpcResp) {
            HANDLE_COMPLETENESS(rpcResp);
            // Ok, merge DataSets to one
            nebula::DataSet v;
            for (auto &resp : rpcResp.responses()) {
                auto *props = resp.get_props();
                if (props != nullptr) {
                    v.append(std::move(*props));
                }
            }
            finish(std::move(v));
            return Status::OK();
        });
    return start();
}

}   // namespace graph
}   // namespace nebula
