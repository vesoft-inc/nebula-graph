/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "UpdateExecutor.h"
#include "planner/Mutate.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"


namespace nebula {
namespace graph {

StatusOr<DataSet> UpdateBaseExecutor::handleResult(const DataSet &data) {
    if (yieldNames_.empty() && (data.colNames.empty() || data.colNames.size() == 1)) {
        return Status::OK();
    }

    if (data.colNames.empty() || data.colNames.size() == 1) {
        return Status::Error("Empty return props");
    }

    if (yieldNames_.size() != data.colNames.size() - 1) {
        LOG(ERROR) << "Expect colName size is " << yieldNames_.size()
                   << ", return colName size is " << data.colNames.size() - 1;
        return Status::Error("Wrong return prop size");
    }
    DataSet result;
    result.colNames = std::move(yieldNames_);
    for (auto &row : data.rows) {
        std::vector<Value> columns;
        for (auto i = 1u; i < row.columns.size(); i++) {
            columns.emplace_back(std::move(row.columns[i]));
        }
        result.rows.emplace_back(std::move(columns));
    }
    return result;
}

folly::Future<Status> UpdateVertexExecutor::execute() {
    return updateVertex().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateVertexExecutor::updateVertex() {
    dumpLog();
    auto *uvNode = asNode<UpdateVertex>(node());
    // TODO: need to handle the vid from pipe or var
    auto vIdRet = SchemaUtil::toVertexID(uvNode->getVertex());
    if (!vIdRet.ok()) {
        return vIdRet.status();
    }
    auto vertexId = std::move(vIdRet).value();
    yieldNames_ = uvNode->getYieldNames();
    auto spaceId = qctx_->rctx()->session()->space();
    auto ret = qctx_->schemaMng()->toTagID(spaceId, uvNode->getName());
    if (!ret.ok()) {
        return ret.status();
    }
    auto tagId = ret.value();
    return qctx()->getStorageClient()->updateVertex(spaceId,
                                                    vertexId,
                                                    tagId,
                                                    uvNode->getUpdatedProps(),
                                                    uvNode->getInsertable(),
                                                    uvNode->getReturnProps(),
                                                    uvNode->getCondition())
        .via(runner())
        .then([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
            if (!resp.ok()) {
                return resp.status();
            }
            auto value = std::move(resp).value();
            if (value.__isset.props) {
                auto status = handleResult(*value.get_props());
                if (!status.ok()) {
                    return status.status();
                }
                finish(std::move(status).value());
            }
            return Status::OK();
        });
}

folly::Future<Status> UpdateEdgeExecutor::execute() {
    return updateEdge().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateEdgeExecutor::updateEdge() {
    dumpLog();
    auto *ueNode = asNode<UpdateEdge>(node());
    auto srcIdRet = SchemaUtil::toVertexID(ueNode->getSrcId());
    if (!srcIdRet.ok()) {
        return srcIdRet.status();
    }
    auto dstIdRet = SchemaUtil::toVertexID(ueNode->getDstId());
    if (!dstIdRet.ok()) {
        return dstIdRet.status();
    }

    auto spaceId = qctx_->rctx()->session()->space();
    auto ret = qctx_->schemaMng()->toEdgeType(spaceId, ueNode->getName());
    if (!ret.ok()) {
        return ret.status();
    }
    auto edgeType = ret.value();

    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(std::move(srcIdRet).value());
    edgeKey.set_ranking(ueNode->getRank());
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_dst(std::move(dstIdRet).value());
    yieldNames_ = ueNode->getYieldNames();

    return qctx()->getStorageClient()->updateEdge(spaceId,
                                                  edgeKey,
                                                  ueNode->getUpdatedProps(),
                                                  ueNode->getInsertable(),
                                                  ueNode->getReturnProps(),
                                                  ueNode->getCondition())
            .via(runner())
            .then([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
                if (!resp.ok()) {
                    return resp.status();
                }
                auto value = std::move(resp).value();
                if (value.__isset.props) {
                    auto status = handleResult(*value.get_props());
                    if (!status.ok()) {
                        return status.status();
                    }
                    finish(std::move(status).value());
                }
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
