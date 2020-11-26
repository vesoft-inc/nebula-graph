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
    otherStats_ = std::make_unique<std::unordered_map<std::string, std::string>>();
    gv_ = asNode<GetVertices>(node());
    reqDs_.colNames = {kVid};
    auto status = buildVerticesRequestDataSet();
    if (!status.ok()) {
        return error(std::move(status));
    }
    return getVertices();
}

Status GetVerticesExecutor::close() {
    // clear the members
    reqDs_.rows.clear();
    return Executor::close();
}

Status GetVerticesExecutor::buildPathRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = gv_->inputVar();
    VLOG(1) << "GetVertices Input : " << inputVar;
    if (gv_->src() == nullptr) {
        return Status::Error("GetVertices's src is nullptr");
    }
    auto iter = ectx_->getResult(inputVar).iter();
    QueryExpressionContext ctx(ectx_);
    std::unordered_set<std::string> uniqueVid;
    for (; iter->valid(); iter->next()) {
        auto path = gv_->src()->eval(ctx(iter.get()));
        VLOG(1) << "path is :" << path;
        if (!path.isPath()) {
            return Status::Error("GetVertices's Type: %s, should be PATH", path.typeName().c_str());
        }
        auto pathVal = path.getPath();
        for (auto& step : pathVal.steps) {
            auto vid = step.dst.vid;
            auto ret = uniqueVid.emplace(vid);
            if (ret.second) {
                reqDs_.rows.emplace_back(Row({std::move(vid)}));
            }
        }
    }
    return Status::OK();
}

Status GetVerticesExecutor::buildVerticesRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    auto inputVar = gv_->inputVar();
    VLOG(1) << "GetVertices Input : " << inputVar;

    if (gv_->src() == nullptr) {
        return Status::Error("GetVertices's src is nullptr");
    }
    auto iter = ectx_->getResult(inputVar).iter();
    QueryExpressionContext ctx(qctx()->ectx());
    std::unordered_set<Value> uniqueSet;
    for (; iter->valid(); iter->next()) {
        auto src = gv_->src()->eval(ctx(iter.get()));
        VLOG(1) << "src is :" << src;
        if (!SchemaUtil::isValidVid(src, spaceInfo.spaceDesc.vid_type)) {
            LOG(WARNING) << "Mismatched vid type: " << src.type();
            continue;
        }
        if (gv_->dedup()) {
            if (uniqueSet.emplace(src).second) {
                reqDs_.rows.emplace_back(Row({std::move(src)}));
            }
        } else {
            reqDs_.rows.emplace_back(Row({std::move(src)}));
        }
    }

    return Status::OK();
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    GraphStorageClient *storageClient = qctx()->getStorageClient();
    if (reqDs_.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder()
                          .value(Value(DataSet(gv->colNames())))
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
        .thenValue([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), this->gv_->colNamesRef());
        });
}

DataSet GetVerticesExecutor::buildRequestDataSet(const GetVertices* gv) {
    if (gv == nullptr) {
        return nebula::DataSet({kVid});
    }
    // Accept Table such as | $a | $b | $c |... as input which one column indicate src
    auto valueIter = ectx_->getResult(gv->inputVar()).iter();
    VLOG(3) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
    return buildRequestDataSetByVidType(valueIter.get(), gv->src(), gv->dedup());
}

}   // namespace graph
}   // namespace nebula
