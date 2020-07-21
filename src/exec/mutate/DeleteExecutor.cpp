/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DeleteExecutor.h"
#include "planner/Mutate.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "exec/mutate/DeleteExecutor.h"


namespace nebula {
namespace graph {

folly::Future<Status> DeleteVerticesExecutor::execute() {
    dumpLog();
    return deleteVertices();
}

folly::Future<Status> DeleteVerticesExecutor::deleteVertices() {
    dumpLog();

    auto *dvNode = asNode<DeleteVertices>(node());
    auto vidRef = dvNode->getVidRef();
    std::vector<VertexID> vertices;
    if (vidRef != nullptr) {
        auto& inputVar = dvNode->inputVar();
        auto& inputResult = ectx_->getResult(inputVar);
        auto iter = inputResult.iter();
        vertices.reserve(iter->size());
        QueryExpressionContext ctx(ectx_, iter.get());
        for (; iter->valid(); iter->next()) {
            auto val = Expression::eval(vidRef, ctx);
            if (val.isNull() || val.empty()) {
                VLOG(3) << "NULL or EMPTY vid";
                continue;
            }
            if (!val.isStr()) {
                LOG(ERROR) << "Wrong input vid type: " << val << ", value: " << val.toString();
                return Status::Error("Wrong input vid type");
            }
            vertices.emplace_back(val.moveStr());
        }
    }

    if (vertices.empty()) {
        return Status::OK();
    }
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getStorageClient()->deleteVertices(spaceId, std::move(vertices))
        .via(runner())
        .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            return handleResponse(resp, "Delete vertices");
        });
}

folly::Future<Status> DeleteEdgesExecutor::execute() {
    return deleteEdges();
}

folly::Future<Status> DeleteEdgesExecutor::deleteEdges() {
    dumpLog();
    auto *deNode = asNode<DeleteEdges>(node());
    auto edgeKeyRefs = deNode->getEdgeKeyRefs();
    std::vector<storage::cpp2::EdgeKey> edgeKeys;
    if (!edgeKeyRefs.empty()) {
        auto& inputVar = deNode->inputVar();
        auto& inputResult = ectx_->getResult(inputVar);
        auto iter = inputResult.iter();
        if (iter->size() == 0) {
            VLOG(2) << "Empty input";
            return Status::OK();
        }
        edgeKeys.reserve(iter->size());
        QueryExpressionContext ctx(ectx_, iter.get());
        for (; iter->valid(); iter->next()) {
            for (auto &edgeKeyRef : edgeKeyRefs) {
                storage::cpp2::EdgeKey edgeKey;
                auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx);
                if (srcId.isNull() || srcId.empty()) {
                    VLOG(3) << "NULL or EMPTY vid";
                    continue;
                }
                if (!srcId.isStr()) {
                    LOG(ERROR) << "Wrong input srcId type: " << srcId
                               << ", value: " << srcId.toString();
                    return Status::Error("Wrong input srcId type");
                }
                auto dstId = Expression::eval(edgeKeyRef->dstid(), ctx);
                if (!dstId.isStr()) {
                    LOG(ERROR) << "Wrong input dstId type: " << dstId
                               << ", value: " << dstId.toString();
                    return Status::Error("Wrong input dstId type");
                }
                auto rank = Expression::eval(edgeKeyRef->rank(), ctx);
                if (!rank.isInt()) {
                    LOG(ERROR) << "Wrong input rank type: " << rank
                               << ", value: " << rank.toString();
                    return Status::Error("Wrong input rank type");
                }
                DCHECK(edgeKeyRef->type());
                auto type = Expression::eval(edgeKeyRef->type(), ctx);
                if (!type.isInt()) {
                    LOG(ERROR) << "Wrong input edge type: " << type
                               << ", value: " << type.toString();
                    return Status::Error("Wrong input edge type");
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
        return Status::OK();
    }

    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getStorageClient()->deleteEdges(spaceId, std::move(edgeKeys))
            .via(runner())
            .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
                return handleResponse(resp, "Delete edges");
            });
}
}   // namespace graph
}   // namespace nebula
