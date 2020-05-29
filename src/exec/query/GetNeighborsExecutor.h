/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
#define EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_

#include <vector>

#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public Executor {
public:
    GetNeighborsExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("GetNeighborsExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getNeighbors();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
    Status handleResponse(RpcResponse& resps);

    void checkResponseResult(const storage::cpp2::ResponseCommon &resp) const;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
