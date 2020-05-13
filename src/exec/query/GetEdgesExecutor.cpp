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
    nebula::DataSet edges({nebula::_SRC, nebula::_TYPE, nebula::_RANK, nebula::_DST});
    if (!ge->edges().empty()) {
        edges.rows.insert(edges.rows.end(),
                          std::make_move_iterator(ge->edges().begin()),
                          std::make_move_iterator(ge->edges().end()));
    }
    if (ge->src() != nullptr && ge->ranking() != nullptr && ge->dst() != nullptr) {
        auto src = ge->src()->eval();
        auto ranking = ge->ranking()->eval();
        auto dst = ge->dst()->eval();
        if (src.type() != Value::Type::LIST || ranking.type() != Value::Type::LIST ||
            dst.type() != Value::Type::LIST) {
            return error(Status::Error("Invalid edge expression"));
        }
        if (src.getList().values.size() != ranking.getList().values.size() ||
            ranking.getList().values.size() != dst.getList().values.size()) {
            return error(Status::Error("Invalid edge expression"));
        }
        for (std::size_t i = 0; i < src.getList().values.size(); ++i) {
            edges.emplace_back(nebula::Row({
                src.getList().values[i].getVertex().vid,
                ge->type(),
                ranking.getList().values[i].getInt(),
                dst.getList().values[i].getVertex().vid,
            }));
        }
    }
    return client
        ->getProps(ge->space(),
                   std::move(edges),
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
                    v.append(std::move(*props));
                }
            }
            finish(std::move(v));
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
