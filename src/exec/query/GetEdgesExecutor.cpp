/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetEdgesExecutor.h"

#include "common/clients/storage/GraphStorageClient.h"

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
    CHECK_NODE_TYPE(GetEdges);
    dumpLog();

    GraphStorageClient *client = qctx()->getStorageClient();
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
        // Accept Table such as | $a | $b | $c | which indicate src, ranking or dst
        auto valueIter = std::make_unique<SequentialIter>(getSingleInputValue());
        auto expCtx = ExpressionContextImpl(qctx()->ectx(), valueIter.get());
        auto src = ge->src()->eval(expCtx);
        auto ranking = ge->ranking()->eval(expCtx);
        auto dst = ge->dst()->eval(expCtx);
        if (src.type() != Value::Type::LIST || ranking.type() != Value::Type::LIST ||
            dst.type() != Value::Type::LIST) {
            return error(Status::Error("Invalid edge expression"));
        }
        if (src.getList().values.size() != ranking.getList().values.size() ||
            ranking.getList().values.size() != dst.getList().values.size()) {
            return error(Status::Error("Invalid edge expression"));
        }
        for (std::size_t i = 0; i < src.getList().values.size(); ++i) {
            if (!src.getList().values[i].isStr()) {
                return Status::NotSupported("Invalid src id");
            }
            if (!ranking.getList().values[i].isInt()) {
                return Status::NotSupported("Invalid ranking");
            }
            if (!dst.getList().values[i].isStr()) {
                return Status::NotSupported("Invalid src id");
            }
            edges.emplace_back(nebula::Row({
                std::move(src).getList().values[i].getStr(),
                ge->type(),
                std::move(ranking).getList().values[i].getInt(),
                std::move(dst).getList().values[i].getStr(),
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
}

}   // namespace graph
}   // namespace nebula
