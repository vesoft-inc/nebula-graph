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
    nebula::DataSet edges({kSrc, kType, kRank, kDst});
    if (!ge->edges().empty()) {
        edges.rows.insert(edges.rows.end(),
                          std::make_move_iterator(ge->edges().begin()),
                          std::make_move_iterator(ge->edges().end()));
    }
    if (ge->src() != nullptr && ge->ranking() != nullptr && ge->dst() != nullptr) {
        // Accept Table such as | $a | $b | $c | $d |... which indicate src, ranking or dst
        auto valueIter = getSingleInput().iter();
        auto expCtx = ExpressionContextImpl(qctx()->ectx(), valueIter.get());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = ge->src()->eval(expCtx);
            auto ranking = ge->ranking()->eval(expCtx);
            auto dst = ge->dst()->eval(expCtx);
            if (!src.isStr() || !ranking.isInt() || !dst.isStr()) {
                LOG(ERROR) << "Mismatched edge key type";
                return Status::Error("Mismatched edge key type");
            }
            edges.emplace_back(Row({
                std::move(src), ge->type(), std::move(ranking), std::move(dst)
            }));
        }
    }
    return client
        ->getProps(ge->space(),
                   std::move(edges),
                   nullptr,
                   &ge->props(),
                   ge->exprs().empty() ? nullptr : &ge->exprs(),
                   ge->dedup(),
                   ge->orderBy(),
                   ge->limit(),
                   ge->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetPropResponse> rpcResp) {
            auto result = handleCompleteness(rpcResp);
            if (!result.ok()) {
                return std::move(result).status();
            }
            auto state = std::move(result).value();
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
                        if (UNLIKELY(!v.append(std::move(*resp.get_props())))) {
                            // it's impossible according to the interface
                            LOG(WARNING) << "Heterogeneous props dataset";
                            state.setStat(State::Stat::kPartialSuccess);
                        }
                    }
                }
            }
            for (auto &colName : v.colNames) {
                for (auto &c : colName) {
                    if (c == ':') {  // see the column name format in thrift
                        c = '.';
                    }
                }
            }
            return finish(ExecResult::buildGetProp(std::move(v), std::move(state)));
        });
}

}   // namespace graph
}   // namespace nebula
