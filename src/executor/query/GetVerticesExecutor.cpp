/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetVerticesExecutor.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    gv_ = asNode<GetVertices>(node());
    if (gv_->dataKind() == GetVertices::DataKind::kVertex) {
        reqDs_ = buildRequestDataSet();
    } else {
        reqDs_ = buildPathRequestDataSet();
    }
    return getVertices();
}

Status GetVerticesExecutor::close() {
    // clear the members
    reqDs_.rows.clear();
    return Executor::close();
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    GraphStorageClient *storageClient = qctx()->getStorageClient();
    if (reqDs_.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder()
                          .value(Value(DataSet(gv_->colNames())))
                          .iter(Iterator::Kind::kProp)
                          .finish());
    }

    time::Duration getPropsTime;
    return DCHECK_NOTNULL(storageClient)
        ->getProps(gv_->space(),
                   std::move(reqDs_),
                   &gv_->props(),
                   nullptr,
                   gv_->exprs().empty() ? nullptr : &gv_->exprs(),
                   gv_->dedup(),
                   gv_->orderBy(),
                   gv_->limit(),
                   gv_->filter())
        .via(runner())
        .ensure([this, getPropsTime]() {
            SCOPED_TIMER(&execTime_);
            otherStats_.emplace("total_rpc",
                                 folly::stringPrintf("%lu(us)", getPropsTime.elapsedInUSec()));
        })
        .thenValue([this](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), this->gv_->colNamesRef());
        });
}

DataSet GetVerticesExecutor::buildRequestDataSet() {
    if (gv_ == nullptr) {
        return nebula::DataSet({kVid});
    }
    // Accept Table such as | $a | $b | $c |... as input which one column indicate src
    auto valueIter = ectx_->getResult(gv_->inputVar()).iter();
    VLOG(3) << "GV input var: " << gv_->inputVar() << " iter kind: " << valueIter->kind();
    return buildRequestDataSetByVidType(valueIter.get(), gv_->src(), gv_->dedup());
}

DataSet GetVerticesExecutor::buildPathRequestDataSet() {
    if (gv_ == nullptr) {
        return nebula::DataSet({kVid});
    }
    // Accept Table such as | $a | $b | $c |... as input which one column indicate src
    auto valueIter = ectx_->getResult(gv_->inputVar()).iter();
    VLOG(3) << "GV input var: " << gv_->inputVar() << " iter kind: " << valueIter->kind();
    return buildPathRequestDataSetByVidType(valueIter.get(), gv_->src());
}

}   // namespace graph
}   // namespace nebula
