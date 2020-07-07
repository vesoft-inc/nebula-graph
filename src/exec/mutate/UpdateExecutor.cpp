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
        for (auto i = 1u; i < row.values.size(); i++) {
            columns.emplace_back(std::move(row.values[i]));
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
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto value = std::move(resp).value();
            for (auto& code : value.get_result().get_failed_parts()) {
                switch (code.get_code()) {
                    case nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER:
                        return Status::Error("Maybe invalid tag or property in WHEN clause!");
                    case nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER:
                        return Status::Error("Maybe invalid tag or property in SET/YIELD clause!");
                    // case nebula::storage::cpp2::ErrorCode::E_FILTER_OUT:
                    //    break;
                    default:
                        std::string errMsg =
                            folly::stringPrintf("Maybe vertex does not exist, "
                                                "part: %d, error code: %d!",
                                                code.get_part_id(),
                                                static_cast<int32_t>(code.get_code()));
                        LOG(ERROR) << errMsg;
                        return Status::Error(std::move(errMsg));;
                }
            }
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
                for (auto& code : value.get_result().get_failed_parts()) {
                    switch (code.get_code()) {
                        case nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER:
                            return Status::Error("Maybe invalid edge or property in WHEN clause!");
                        case nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER:
                            return Status::Error(
                                    "Maybe invalid edge or property in SET/YIELD clause!");
                        // case nebula::storage::cpp2::ErrorCode::E_FILTER_OUT:
                        //    break;
                        default:
                            std::string errMsg =
                                folly::stringPrintf("Maybe edge does not exist, "
                                                    "part: %d, error code: %d!",
                                                    code.get_part_id(),
                                                    static_cast<int32_t>(code.get_code()));
                            LOG(ERROR) << errMsg;
                            return Status::Error(std::move(errMsg));;
                    }
                }
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
