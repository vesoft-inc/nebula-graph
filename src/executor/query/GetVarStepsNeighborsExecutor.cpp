/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetVarStepsNeighborsExecutor.h"

#include <sstream>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"
#include "service/GraphFlags.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> GetVarStepsNeighborsExecutor::execute() {
    steps_ = gn_->steps();
    auto status = buildRequestDataSet();
    if (!status.ok()) {
        return error(std::move(status));
    }
    return getVarStepsNeighbors();
}

Status GetVarStepsNeighborsExecutor::close() {
    // clear the members
    reqDs_.rows.clear();
    return Executor::close();
}

Status GetVarStepsNeighborsExecutor::buildRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = gn_->inputVar();
    VLOG(1) << node()->outputVar() << " : " << inputVar;
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    QueryExpressionContext ctx(ectx_);
    DataSet input;
    reqDs_.colNames = {kVid};
    reqDs_.rows.reserve(iter->size());
    auto* src = DCHECK_NOTNULL(gn_->src());
    std::unordered_set<Value> uniqueVid;
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    for (; iter->valid(); iter->next()) {
        auto val = Expression::eval(src, ctx(iter.get()));
        if (!SchemaUtil::isValidVid(val, *(spaceInfo.spaceDesc.vid_type_ref()))) {
            continue;
        }
        if (gn_->dedup()) {
            auto ret = uniqueVid.emplace(val);
            if (ret.second) {
                reqDs_.rows.emplace_back(Row({std::move(val)}));
            }
        } else {
            reqDs_.rows.emplace_back(Row({std::move(val)}));
        }
    }
    return Status::OK();
}

folly::Future<Status> GetVarStepsNeighborsExecutor::getVarStepsNeighbors() {
    if (reqDs_.rows.empty()) {
        VLOG(1) << "Empty input.";
        List emptyResult;
        return finish(ResultBuilder()
                          .value(Value(std::move(emptyResult)))
                          .iter(Iterator::Kind::kGetNeighbors)
                          .finish());
    }
    getNeighbors();
    return promise_.getFuture();
}

void GetVarStepsNeighborsExecutor::getNeighbors() {
    currentStep_++;
    time::Duration getNbrTime;
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    bool finalStep = isFinalStep();
    storageClient
        ->getNeighbors(gn_->space(),
                       reqDs_.colNames,
                       std::move(reqDs_.rows),
                       gn_->edgeTypes(),
                       gn_->edgeDirection(),
                       finalStep || gn_->needUnion() ? gn_->statProps() : nullptr,
                       finalStep || gn_->needUnion() ? gn_->vertexProps() : nullptr,
                       finalStep || gn_->needUnion() ? gn_->edgeProps() : gn_->edgeDst(),
                       finalStep || gn_->needUnion() ? gn_->exprs() : nullptr,
                       finalStep || gn_->needUnion() ? gn_->dedup() : false,
                       finalStep || gn_->needUnion() ? gn_->random() : false,
                       finalStep || gn_->needUnion() ? gn_->orderBy() : nullptr,
                       finalStep || gn_->needUnion() ? gn_->limit() : -1,
                       finalStep || gn_->needUnion() ? gn_->filter() : "")
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
            handleResponse(resp);
        });
}

void GetVarStepsNeighborsExecutor::handleResponse(RpcResponse& resps) {
    SCOPED_TIMER(&execTime_);
    auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
    if (!result.ok()) {
        promise_.setValue(std::move(result).status());
    }

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
    auto listVal = std::make_shared<Value>(std::move(list));
    auto iter = std::make_unique<GetNeighborsIter>(listVal);
    VLOG(1) << "curr step: " << currentStep_ << " steps: " << steps_.nSteps();
    const auto& spaceInfo = qctx()->rctx()->session()->space();

    if (!isFinalStep()) {
        DataSet reqDs;
        reqDs.colNames = reqDs_.colNames;
        std::unordered_set<Value> uniqueVid;
        for (; iter->valid(); iter->next()) {
            auto& vid = iter->getEdgeProp("*", kDst);
            if (!SchemaUtil::isValidVid(vid, *(spaceInfo.spaceDesc.vid_type_ref()))) {
                continue;
            }
            if (uniqueVid.emplace(vid).second) {
                reqDs.rows.emplace_back(Row({std::move(vid)}));
            }
        }
        reqDs_ = std::move(reqDs);
        buildResult(listVal, iter.get(), result.value());
        if (reqDs_.rows.empty()) {
            VLOG(1) << "Empty input.";
            if (steps_.isMToN()) {
                promise_.setValue(Status::OK());
            } else {
                List emptyResult;
                promise_.setValue(finish(ResultBuilder()
                                            .value(Value(std::move(emptyResult)))
                                            .iter(Iterator::Kind::kGetNeighbors)
                                            .finish()));
            }
        } else {
            getNeighbors();
        }
    } else {
        buildResult(listVal, iter.get(), result.value());
        promise_.setValue(Status::OK());
    }
}

void GetVarStepsNeighborsExecutor::buildResult(std::shared_ptr<Value> listVal,
                                               GetNeighborsIter* iter,
                                               Result::State state) {
    if (steps_.isMToN() && currentStep_ >= steps_.mSteps()) {
        auto&& list = listVal->moveList();
        unionAllResult_.values.insert(unionAllResult_.values.end(),
                                      std::make_move_iterator(list.values.begin()),
                                      std::make_move_iterator(list.values.end()));
        VLOG(1) << "result size: " << unionAllResult_.values.size();
        if (isFinalStep() || reqDs_.rows.empty()) {
            VLOG(1) << "finish." << unionAllResult_;
            finish(ResultBuilder()
                       .value(std::make_shared<Value>(std::move(unionAllResult_)))
                       .iter(Iterator::Kind::kGetNeighbors)
                       .finish());
        }
    } else {
        ResultBuilder builder;
        builder.state(state);
        builder.iter(iter->copy());
        finish(builder.finish());
    }
}

}   // namespace graph
}   // namespace nebula
