/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DeleteEdgesExecutor.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"


namespace nebula {
namespace graph {

folly::Future<Status> DeleteEdgesExecutor::execute() {
    return deleteEdges();
}

folly::Future<Status> DeleteEdgesExecutor::deleteEdges() {
    dumpLog();

    auto *deNode = asNode<DeleteEdges>(node());
    auto status = prepareEdgeKeys(deNode->getEdgeType(), deNode->getEdgeKeys());
    if (!status.ok()) {
        return status;
    }

    return qctx()->getStorageClient()->deleteEdges(deNode->getSpace(), std::move(edgeKeys_))
        .via(runner())
        .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            auto completeness = resp.completeness();
            if (completeness != 100) {
                const auto& failedCodes = resp.failedParts();
                for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                    LOG(ERROR) << "Delete edges failed, error "
                               << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                               << ", part " << it->first;
                }
                return Status::Error("Delete edges not complete, completeness: %d",
                                      completeness);
            }
            return Status::OK();
        });
}

Status DeleteEdgesExecutor::prepareEdgeKeys(const EdgeType edgeType,
                                            const EdgeKeys *edgeKeys) {
    auto edges = edgeKeys->keys();
    for (auto *edge : edges) {
        auto rank = edge->rank();
        auto srcIdRet = SchemaUtil::toVertexID(edge->srcid());
        if (!srcIdRet.ok()) {
            return srcIdRet.status();
        }
        auto srcId = std::move(srcIdRet).value();

        auto dstIdRet = SchemaUtil::toVertexID(edge->dstid());
        if (!dstIdRet.ok()) {
            return dstIdRet.status();
        }
        auto dstId = std::move(dstIdRet).value();

        storage::cpp2::EdgeKey outKey;
        outKey.set_src(srcId);
        outKey.set_edge_type(edgeType);
        outKey.set_dst(dstId);
        outKey.set_ranking(rank);

        storage::cpp2::EdgeKey inKey;
        inKey.set_src(dstId);
        inKey.set_edge_type(-edgeType);
        inKey.set_dst(srcId);
        inKey.set_ranking(rank);

        edgeKeys_.emplace_back(std::move(outKey));
        edgeKeys_.emplace_back(std::move(inKey));
    }
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
