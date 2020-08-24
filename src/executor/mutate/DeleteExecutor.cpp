/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DeleteExecutor.h"
#include "planner/Mutate.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "executor/mutate/DeleteExecutor.h"
#include "util/ScopedTimer.h"


namespace nebula {
namespace graph {

folly::Future<GraphStatus> DeleteVerticesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return deleteVertices();
}

folly::Future<GraphStatus> DeleteVerticesExecutor::deleteVertices() {
    auto *dvNode = asNode<DeleteVertices>(node());
    auto vidRef = dvNode->getVidRef();
    std::vector<VertexID> vertices;
    if (vidRef != nullptr) {
        auto inputVar = dvNode->inputVar();
        // empty inputVar means using pipe and need to get the GetNeighbors's inputVar
        if (inputVar.empty()) {
            DCHECK(dvNode->dep() != nullptr);
            auto* gn = static_cast<const SingleInputNode*>(dvNode->dep())->dep();
            DCHECK(gn != nullptr);
            inputVar = static_cast<const SingleInputNode*>(gn)->inputVar();
        }
        DCHECK(!inputVar.empty());
        VLOG(2) << "inputVar: " << inputVar;
        auto& inputResult = ectx_->getResult(inputVar);
        auto iter = inputResult.iter();
        vertices.reserve(iter->size());
        QueryExpressionContext ctx(ectx_);
        for (; iter->valid(); iter->next()) {
            auto val = Expression::eval(vidRef, ctx(iter.get()));
            if (val.isNull() || val.empty()) {
                VLOG(3) << "NULL or EMPTY vid";
                continue;
            }
            if (!val.isStr()) {
                LOG(ERROR) << "Wrong vid type `" << val.type()
                           << "', value `" << val.toString() << "'";
                return GraphStatus::setInvalidVid();
            }
            vertices.emplace_back(val.moveStr());
        }
    }

    if (vertices.empty()) {
        return GraphStatus::OK();
    }
    auto spaceId = qctx()->rctx()->session()->space();
    time::Duration deleteVertTime;
    return qctx()->getStorageClient()->deleteVertices(spaceId, std::move(vertices))
        .via(runner())
        .ensure([deleteVertTime]() {
            VLOG(1) << "Delete vertices time: " << deleteVertTime.elapsedInUSec() << "us";
        })
        .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            SCOPED_TIMER(&execTime_);
            return handleResponse(resp, "Delete vertices");
        });
}

folly::Future<GraphStatus> DeleteEdgesExecutor::execute() {
    return deleteEdges();
}

folly::Future<GraphStatus> DeleteEdgesExecutor::deleteEdges() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DeleteEdges>(node());
    auto edgeKeyRefs = deNode->getEdgeKeyRefs();
    std::vector<storage::cpp2::EdgeKey> edgeKeys;
    if (!edgeKeyRefs.empty()) {
        auto& inputVar = deNode->inputVar();
        DCHECK(!inputVar.empty());
        auto& inputResult = ectx_->getResult(inputVar);
        auto iter = inputResult.iter();
        if (iter->size() == 0) {
            VLOG(2) << "Empty input";
            return GraphStatus::OK();
        }
        edgeKeys.reserve(iter->size());
        QueryExpressionContext ctx(ectx_);
        for (; iter->valid(); iter->next()) {
            for (auto &edgeKeyRef : edgeKeyRefs) {
                storage::cpp2::EdgeKey edgeKey;
                auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx(iter.get()));
                if (srcId.isNull() || srcId.empty()) {
                    VLOG(3) << "NULL or EMPTY vid";
                    continue;
                }
                if (!srcId.isStr()) {
                    LOG(ERROR) << "Wrong srcId type `" << srcId.type()
                               << "`, value `" << srcId.toString() << "'";
                    return GraphStatus::setInvalidVid();
                }
                auto dstId = Expression::eval(edgeKeyRef->dstid(), ctx(iter.get()));
                if (!dstId.isStr()) {
                    LOG(ERROR) << "Wrong dstId type `" << dstId.type()
                               << "', value `" << dstId.toString() << "'";
                    return GraphStatus::setInvalidVid();
                }
                auto rank = Expression::eval(edgeKeyRef->rank(), ctx(iter.get()));
                if (!rank.isInt()) {
                    LOG(ERROR) << "Wrong rank type `" << rank.type()
                               << "', value `" << rank.toString() << "'";
                    return GraphStatus::setInvalidRank();
                }
                DCHECK(edgeKeyRef->type());
                auto type = Expression::eval(edgeKeyRef->type(), ctx(iter.get()));
                if (!type.isInt()) {
                    LOG(ERROR) << "Wrong edge type `" << type.type()
                               << "', value `" << type.toString() << "'";
                    return GraphStatus::setInvalidEdgeType();
                }

                // out edge
                edgeKey.set_src(srcId.getStr());
                edgeKey.set_dst(dstId.getStr());
                edgeKey.set_ranking(rank.getInt());
                edgeKey.set_edge_type(type.getInt());
                edgeKeys.emplace_back(edgeKey);

                // in edge
                edgeKey.set_src(dstId.moveStr());
                edgeKey.set_dst(srcId.moveStr());
                edgeKey.set_edge_type(-type.getInt());
                edgeKeys.emplace_back(std::move(edgeKey));
            }
        }
    }

    if (edgeKeys.empty()) {
        VLOG(2) << "Empty edgeKeys";
        return GraphStatus::OK();
    }

    auto spaceId = qctx()->rctx()->session()->space();
    time::Duration deleteEdgeTime;
    return qctx()->getStorageClient()->deleteEdges(spaceId, std::move(edgeKeys))
            .via(runner())
            .ensure([deleteEdgeTime]() {
                VLOG(1) << "Delete edge time: " << deleteEdgeTime.elapsedInUSec() << "us";
            })
            .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
                SCOPED_TIMER(&execTime_);
                return handleResponse(resp, "Delete edges");
            });
}
}   // namespace graph
}   // namespace nebula
