/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetVerticesExecutor.h"
#include <boost/variant/variant.hpp>

#include "common/clients/storage/GraphStorageClient.h"

#include "planner/Query.h"
#include "context/QueryContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
    return getVertices().ensure([this]() {
        // TODO(yee): some cleanup or stats actions
        UNUSED(this);
    });
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    CHECK_NODE_TYPE(GetVertices)
    dumpLog();

    auto *gv = asNode<GetVertices>(node());

    GraphStorageClient *storageClient = qctx()->getStorageClient();
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
        // Accept Table such as | $a | $b | $c |... as input which one column indicate src
        auto valueIter = std::make_unique<SequentialIter>(getSingleInputValue());
        auto expCtx = ExpressionContextImpl(qctx()->ectx(), valueIter.get());
        auto src = gv->src()->eval(expCtx);
        // a table such as |$exp|
        DataSet &input = src.mutableDataSet();
        input.colNames = vertices.colNames;
        vertices.append(std::move(input));
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
            if (!rpcResp.responses().empty()) {
                if (rpcResp.responses().front().__isset.props) {
                    v = std::move(*rpcResp.responses().front().get_props());
                }
            }
            if (rpcResp.responses().size() > 1) {
                for (std::size_t i = 1; i < rpcResp.responses().size(); ++i) {
                    auto resp = rpcResp.responses()[i];
                    if (resp.__isset.props) {
                        v.append(std::move(*resp.get_props()));
                    }
                }
            }
            finish(std::move(v));
            return Status::OK();
        });
    return start();
}

}   // namespace graph
}   // namespace nebula
