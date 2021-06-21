/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
#define EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_

#include "common/interface/gen-cpp2/storage_types.h"

#include <tuple>

#include "executor/StorageAccessExecutor.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public StorageAccessExecutor {
public:
    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
    using StatefulResp = std::tuple<List, Result::State>;

    GetNeighborsExecutor(const PlanNode* node, QueryContext* qctx)
        : StorageAccessExecutor("GetNeighborsExecutor", node, qctx) {
        gn_ = asNode<GetNeighbors>(node);
    }

    folly::Future<Status> execute() override;

    DataSet buildRequestDataSet();

private:
    StatusOr<StatefulResp> handleResponse(RpcResponse& resps);
    folly::Future<StatusOr<StatefulResp>> execute(DataSet ds);

private:
    const GetNeighbors* gn_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
