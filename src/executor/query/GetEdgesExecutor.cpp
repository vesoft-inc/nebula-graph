/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetEdgesExecutor.h"
#include "context/QueryContext.h"
#include "planner/Query.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    otherStats_ = std::make_unique<std::unordered_map<std::string, std::string>>();
    ge_ = asNode<GetEdges>(node());
    reqDs_.colNames = {kSrc, kType, kRank, kDst};
    auto status = buildEdgeRequestDataSet();
    if (!status.ok()) {
        return error(std::move(status));
    }
    return getEdges();
}

Status GetEdgesExecutor::close() {
    // clear the members
    reqDs_.rows.clear();
    return Executor::close();
}

Status GetEdgesExecutor::buildEdgeRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    const auto &spaceInfo = qctx()->rctx()->session()->space();
    auto inputVar = ge_->inputVar();
    VLOG(1) << "GetEdges Input : " << inputVar;
    if (ge_->src() != nullptr && ge_->type() != nullptr && ge_->ranking() != nullptr &&
        ge_->dst() != nullptr) {
        // Accept Table such as | $a | $b | $c | $d |... which indicate src, ranking or dst
        auto iter = ectx_->getResult(inputVar).iter();
        QueryExpressionContext ctx(ectx_);
        for (; iter->valid(); iter->next()) {
            auto src = ge_->src()->eval(ctx(iter.get()));
            auto type = ge_->type()->eval(ctx(iter.get()));
            auto ranking = ge_->ranking()->eval(ctx(iter.get()));
            auto dst = ge_->dst()->eval(ctx(iter.get()));
            if (!SchemaUtil::isValidVid(src, spaceInfo.spaceDesc.vid_type) ||
                !SchemaUtil::isValidVid(dst, spaceInfo.spaceDesc.vid_type) || !type.isInt() ||
                !ranking.isInt()) {
                LOG(WARNING) << "Mismatched edge key type";
                continue;
            }
            reqDs_.emplace_back(Row({std::move(src), type, ranking, std::move(dst)}));
        }
    }
    return Status::OK();
}

Status GetEdgesExecutor::buildPathRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = ge_->inputVar();
    VLOG(1) << "GetEdges Input : " << inputVar;
    if (ge_->src() == nullptr) {
        return Status::Error("GetEdges Path's src is nullptr");
    }
    auto iter = ectx_->getResult(inputVar).iter();
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        auto path = ge_->src()->eval(ctx(iter.get()));
        VLOG(1) << "path is :" << path;
        if (!path.isPath()) {
            return Status::Error("GetEdges's Type : %s, should be PATH", path.typeName().c_str());
        }
        auto pathVal = path.getPath();
        auto src = pathVal.src.vid;
        for (auto &step : pathVal.steps) {
            auto type = step.type;
            auto ranking = step.ranking;
            auto dst = step.dst.vid;
            reqDs_.emplace_back(Row({std::move(src), type, ranking, dst}));
            src = dst;
        }
    }
    return Status::OK();
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    GraphStorageClient *client = qctx()->getStorageClient();
    if (reqDs_.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder()
                          .value(Value(DataSet(ge->colNames())))
                          .iter(Iterator::Kind::kProp)
                          .finish());
    }

    time::Duration getPropsTime;
    return DCHECK_NOTNULL(client)
        ->getProps(ge_->space(),
                   std::move(reqDs_),
                   nullptr,
                   &ge_->props(),
                   ge_->exprs().empty() ? nullptr : &ge_->exprs(),
                   ge_->dedup(),
                   ge_->orderBy(),
                   ge_->limit(),
                   ge_->filter())
        .via(runner())
        .ensure([this, getPropsTime]() {
            SCOPED_TIMER(&execTime_);
            otherStats_.emplace("total_rpc",
                                folly::stringPrintf("%lu(us)", getPropsTime.elapsedInUSec()));
        })
        .thenValue([this, ge](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), this->ge_->colNamesRef());
        });
}

}   // namespace graph
}   // namespace nebula
