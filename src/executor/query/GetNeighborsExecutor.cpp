/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetNeighborsExecutor.h"

#include <sstream>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"
#include "util/SchemaUtil.h"
#include "service/GraphFlags.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

DataSet GetNeighborsExecutor::buildRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = gn_->inputVar();
    VLOG(1) << node()->outputVar() << " : " << inputVar;
    auto iter = ectx_->getResult(inputVar).iter();
    return buildRequestDataSetByVidType(iter.get(), gn_->dedup());
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

    time::Duration getNbrTime;
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    return storageClient
        ->getNeighbors(gn_->space(),
                       std::move(reqDs.colNames),
                       std::move(reqDs.rows),
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
        .ensure([this, getNbrTime]() {
            SCOPED_TIMER(&execTime_);
            otherStats_.emplace("total_rpc_time",
                                folly::stringPrintf("%lu(us)", getNbrTime.elapsedInUSec()));
        })
        .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            SCOPED_TIMER(&execTime_);
            auto& hostLatency = resp.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); ++i) {
                auto& info = hostLatency[i];
                otherStats_.emplace(
                        folly::stringPrintf("%s exec/total/vertices",
                                            std::get<0>(info).toString().c_str()),
                        folly::stringPrintf("%d(us)/%d(us)/%lu,",
                                            std::get<1>(info),
                                            std::get<2>(info),
                                            (*resp.responses()[i].vertices_ref()).size()));
            }
            return handleResponse(resp);
        });
}

Status GetNeighborsExecutor::handleResponse(RpcResponse& resps) {
    auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
    NG_RETURN_IF_ERROR(result);
    ResultBuilder builder;
    builder.state(result.value());

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
    builder.value(Value(std::move(list)));
    return finish(builder.iter(Iterator::Kind::kGetNeighbors).finish());
}

DataSet GetNeighborsExecutor::buildRequestDataSet(const SpaceInfo &space,
                                                  QueryExpressionContext &exprCtx,
                                                  Iterator *iter,
                                                  const std::vector<std::string> &colNames,
                                                  const std::vector<Expression *> &exprs,
                                                  bool dedup) {
    DCHECK(iter) << "iter=" << iter;
    DCHECK_EQ(colNames.size(), exprs.size());
    nebula::DataSet vertices(colNames);
    vertices.rows.reserve(iter->size());

    std::unordered_set<Row> uniqueSet;
    uniqueSet.reserve(iter->size());

    const auto &vidType = *(space.spaceDesc.vid_type_ref());

    for (; iter->valid(); iter->next()) {
        Row row;
        for (const auto &expr : exprs) {
            auto vid = expr->eval(exprCtx(iter));
            if (!SchemaUtil::isValidVid(vid, vidType)) {
                LOG(WARNING) << "Mismatched vid type: " << vid.type()
                            << ", space vid type: " << SchemaUtil::typeToString(vidType);
                continue;
            }
            row.emplace_back(std::move(vid));
        }
        if (row.size() != vertices.colSize()) {
            // Contains invalid data
            continue;
        }
        if (dedup && !uniqueSet.emplace(row).second) {
            continue;
        }
        vertices.emplace_back(std::move(row));
    }
    return vertices;
}

bool GetNeighborsExecutor::isIntVidType(const SpaceInfo &space) const {
    return (*space.spaceDesc.vid_type_ref()).type == meta::cpp2::PropertyType::INT64;
}

DataSet GetNeighborsExecutor::buildRequestDataSetByVidType(Iterator *iter,
                                                            bool dedup) {
    const auto &space = qctx()->rctx()->session()->space();
    QueryExpressionContext exprCtx(qctx()->ectx());
    std::vector<std::string> colNames{kVid};
    if (gn_->dst() != nullptr) {
        colNames.emplace_back(kDst);
    }
    std::vector<Expression*> exprs{gn_->src()};
    if (gn_->dst() != nullptr) {
        exprs.emplace_back(gn_->dst());
    }

    if (isIntVidType(space)) {
        return buildRequestDataSet(space, exprCtx, iter, colNames, exprs, dedup);
    }
    return buildRequestDataSet(space, exprCtx, iter, colNames, exprs, dedup);
}

}   // namespace graph
}   // namespace nebula
