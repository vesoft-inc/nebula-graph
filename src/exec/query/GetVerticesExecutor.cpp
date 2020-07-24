/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetVerticesExecutor.h"
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
    dumpLog();

    auto *gv = asNode<GetVertices>(node());

    GraphStorageClient *storageClient = qctx()->getStorageClient();
    nebula::DataSet vertices({kVid});
    std::unordered_set<Value> uniqueVid;
    if (!gv->vertices().empty()) {
        for (auto& v : gv->vertices()) {
            auto ret = uniqueVid.emplace(v.values.front());
            if (ret.second) {
                vertices.emplace_back(std::move(v));
            }
        }
        vertices.rows.insert(vertices.rows.end(),
                             std::make_move_iterator(gv->vertices().begin()),
                             std::make_move_iterator(gv->vertices().end()));
    }
    if (gv->src() != nullptr) {
        // Accept Table such as | $a | $b | $c |... as input which one column indicate src
        auto valueIter = ectx_->getResult(gv->inputVar()).iter();
        VLOG(1) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
        auto expCtx = QueryExpressionContext(qctx()->ectx(), valueIter.get());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = gv->src()->eval(expCtx);
            VLOG(1) << "src vid: " << src;
            if (!src.isStr()) {
                LOG(WARNING) << "Mismatched vid type: " << src.type();
                continue;
            }
            auto ret = uniqueVid.emplace(src);
            if (ret.second) {
                vertices.emplace_back(Row({std::move(src)}));
            }
        }
    }

    if (vertices.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder().value(Value(DataSet())).finish());
    }

    return DCHECK_NOTNULL(storageClient)
        ->getProps(gv->space(),
                   std::move(vertices),
                   &gv->props(),
                   nullptr,
                   gv->exprs().empty() ? nullptr : &gv->exprs(),
                   gv->dedup(),
                   gv->orderBy(),
                   gv->limit(),
                   gv->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            return handleResp(std::move(rpcResp));
        });
    return start();
}

}   // namespace graph
}   // namespace nebula
