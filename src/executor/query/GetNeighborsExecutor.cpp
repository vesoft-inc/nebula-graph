/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetNeighborsExecutor.h"

#include <iterator>
#include <sstream>

#include "common/base/Status.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "context/Iterator.h"
#include "context/QueryContext.h"
#include "context/Result.h"
#include "service/GraphFlags.h"
#include "util/ScopedTimer.h"

DEFINE_int32(get_nbr_batch_size, 128, "batch size each request in get neighbors");

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

using StatefulResp = nebula::graph::GetNeighborsExecutor::StatefulResp;

namespace nebula {
namespace graph {

DataSet GetNeighborsExecutor::buildRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = gn_->inputVar();
    VLOG(1) << node()->outputVar() << " : " << inputVar;
    auto iter = ectx_->getResult(inputVar).iter();
    return buildRequestDataSetByVidType(iter.get(), gn_->src(), gn_->dedup());
}

folly::Future<StatusOr<StatefulResp>> GetNeighborsExecutor::execute(DataSet ds) {
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    return storageClient
        ->getNeighbors(gn_->space(),
                       std::move(ds.colNames),
                       std::move(ds.rows),
                       gn_->edgeTypes(),
                       gn_->edgeDirection(),
                       gn_->statProps(),
                       gn_->vertexProps(),
                       gn_->edgeProps(),
                       gn_->exprs(),
                       gn_->dedup(),
                       gn_->random(),
                       gn_->orderBy(),
                       gn_->limit(),
                       gn_->filter())
        .via(runner())
        .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            SCOPED_TIMER(&execTime_);
            auto& hostLatency = resp.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); ++i) {
                size_t size = 0u;
                auto& result = resp.responses()[i];
                if (result.vertices_ref().has_value()) {
                    size = (*result.vertices_ref()).size();
                }
                auto& info = hostLatency[i];
                otherStats_.emplace(
                    folly::stringPrintf("%s exec/total/vertices",
                                        std::get<0>(info).toString().c_str()),
                    folly::stringPrintf(
                        "%d(us)/%d(us)/%lu,", std::get<1>(info), std::get<2>(info), size));
            }
            return handleResponse(resp);
        });
}

DataSet rangeDataSet(const DataSet& ds, size_t from, size_t sz) {
    DataSet dataset(ds.colNames);
    dataset.rows.reserve(sz);
    for (size_t i = from, e = from + sz; i < e; i++) {
        dataset.rows.emplace_back(std::move(ds.rows[i]));
    }
    return dataset;
}

folly::Future<Status> GetNeighborsExecutor::execute() {
    DataSet reqDs = buildRequestDataSet();
    if (reqDs.rows.empty()) {
        List emptyResult;
        return finish(ResultBuilder()
                          .value(Value(std::move(emptyResult)))
                          .iter(Iterator::Kind::kGetNeighbors)
                          .finish());
    }

    auto numRows = reqDs.size();
    auto nBatches = numRows / FLAGS_get_nbr_batch_size;
    std::vector<folly::Future<StatusOr<StatefulResp>>> futures;
    futures.reserve(nBatches + 1);
    for (size_t i = 0; i < nBatches; ++i) {
        auto from = i * FLAGS_get_nbr_batch_size;
        futures.emplace_back(execute(rangeDataSet(reqDs, from, FLAGS_get_nbr_batch_size)));
    }

    auto rem = numRows % FLAGS_get_nbr_batch_size;
    if (rem != 0) {
        auto from = nBatches * FLAGS_get_nbr_batch_size;
        futures.emplace_back(execute(rangeDataSet(reqDs, from, rem)));
    }

    // time::Duration getNbrTime;
    return folly::collect(futures).via(runner()).thenValue(
        [this](std::vector<StatusOr<StatefulResp>> stats) {
            // TODO(yee): Add profiling data
            // SCOPED_TIMER(&getNbrTime);

            Result::State state = Result::State::kSuccess;
            size_t sz = 0;
            for (auto& stat : stats) {
                NG_RETURN_IF_ERROR(stat);
                const auto& tup = stat.value();
                if (std::get<Result::State>(tup) != Result::State::kSuccess) {
                    state = std::get<Result::State>(tup);
                }
                sz += std::get<List>(tup).size();
            }

            List list;
            list.values.reserve(sz);
            for (auto& stat : stats) {
                auto tup = std::move(stat).value();
                auto& vals = std::get<List>(tup).values;
                list.values.insert(list.values.end(),
                                   std::make_move_iterator(vals.begin()),
                                   std::make_move_iterator(vals.end()));
            }

            return finish(ResultBuilder()
                              .state(state)
                              .value(Value(std::move(list)))
                              .iter(Iterator::Kind::kGetNeighbors)
                              .finish());
        });
}

StatusOr<StatefulResp> GetNeighborsExecutor::handleResponse(RpcResponse& resps) {
    auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
    NG_RETURN_IF_ERROR(result);

    auto& responses = resps.responses();
    VLOG(1) << "Resp size: " << responses.size();
    List list;
    for (auto& resp : responses) {
        auto dataset = resp.get_vertices();
        if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
        }

        VLOG(1) << "Resp row size: " << dataset->rows.size() << "Resp : " << *dataset;
        list.values.emplace_back(std::move(*dataset));
    }
    return std::make_tuple(std::move(list), std::move(result).value());
}

}   // namespace graph
}   // namespace nebula
