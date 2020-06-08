/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DeleteVerticesExecutor.h"
#include "planner/Mutate.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"


namespace nebula {
namespace graph {

folly::Future<Status> DeleteVerticesExecutor::execute() {
    dumpLog();
    return deleteVertices();
}

folly::Future<Status> DeleteVerticesExecutor::deleteVertices() {
    dumpLog();

    auto *dvNode = asNode<DeleteVertices>(node());
    auto vertices = dvNode->getVertices();
    for (auto vertex : vertices) {
        auto vIdRet = SchemaUtil::toVertexID(vertex);
        if (!vIdRet.ok()) {
            return vIdRet.status();
        }
        vertices_.emplace_back(std::move(vIdRet).value());
    }

    return qctx()->getStorageClient()->deleteVertices(space_, vertices_)
        .via(runner())
        .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            auto completeness = resp.completeness();
            if (completeness != 100) {
                const auto& failedCodes = resp.failedParts();
                for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                    LOG(ERROR) << "Delete vertices failed, error "
                               << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                               << ", part " << it->first;
                }
                return Status::Error("Delete vertices not complete, completeness: %d",
                                      completeness);
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
