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

Status UpdateBaseExecutor::handleErrorCode(nebula::storage::cpp2::ErrorCode code,
                                           PartitionID partId) {
    switch (code) {
        case storage::cpp2::ErrorCode::E_INVALID_FIELD_VALUE:
            return Status::Error(
                    "Invalid field value: may be the filed without default value or wrong schema");
        case storage::cpp2::ErrorCode::E_INVALID_FILTER:
            return Status::Error("Invalid filter");
        case storage::cpp2::ErrorCode::E_INVALID_UPDATER:
            return Status::Error("Invalid Update col or yield col");
        case storage::cpp2::ErrorCode::E_TAG_NOT_FOUND:
            return Status::Error("Tag `%s' not found.", name_.c_str());
        case storage::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
            return Status::Error("Edge `%s' not found.", name_.c_str());
        case storage::cpp2::ErrorCode::E_INVALID_DATA:
            return Status::Error("Invalid data, may be wrong value type");
        case storage::cpp2::ErrorCode::E_FILTER_OUT:
            return Status::OK();
        default:
            std::string errMsg = folly::stringPrintf("Unknown error, part: %d, error code: %d.",
                                                      partId, static_cast<int32_t>(code));
            LOG(ERROR) << errMsg;
            return Status::Error(std::move(errMsg));;
    }
    return Status::OK();
}

folly::Future<Status> UpdateVertexExecutor::execute() {
    return updateVertex().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateVertexExecutor::updateVertex() {
    dumpLog();
    auto *uvNode = asNode<UpdateVertex>(node());
    name_ = uvNode->getName();
    yieldNames_ = uvNode->getYieldNames();
    auto spaceId = qctx_->rctx()->session()->space();
    auto ret = qctx_->schemaMng()->toTagID(spaceId, uvNode->getName());
    if (!ret.ok()) {
        return ret.status();
    }
    auto tagId = ret.value();
    return qctx()->getStorageClient()->updateVertex(spaceId,
                                                    uvNode->getVId(),
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
                NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
            }
            if (value.__isset.props) {
                auto status = handleResult(*value.get_props());
                if (!status.ok()) {
                    return status.status();
                }
                return finish(ResultBuilder()
                                  .value(std::move(status).value())
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
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
    auto spaceId = qctx_->rctx()->session()->space();
    name_ = ueNode->getName();
    auto ret = qctx_->schemaMng()->toEdgeType(spaceId, name_);
    if (!ret.ok()) {
        return ret.status();
    }
    auto edgeType = ret.value();

    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(ueNode->getSrcId());
    edgeKey.set_ranking(ueNode->getRank());
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_dst(ueNode->getDstId());
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
                    NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
                }
                if (value.__isset.props) {
                    auto status = handleResult(*value.get_props());
                    if (!status.ok()) {
                        return status.status();
                    }
                    return finish(ResultBuilder()
                                    .value(std::move(status).value())
                                    .iter(Iterator::Kind::kDefault)
                                    .finish());
                }
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
