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
#include "exec/Executor.h"
#include "common/interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public Executor {
public:
<<<<<<< HEAD
    GetNeighborsExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("GetNeighborsExecutor", node, ectx) {}
=======
    GetNeighborsExecutor(const PlanNode *node, QueryContext *qctx, Executor *input)
        : SingleInputExecutor("GetNeighborsExecutor", node, qctx, input) {}
>>>>>>> Replace ExecutionContext with QueryContext.

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getNeighbors();

    Status handleResponse(const std::vector<storage::cpp2::GetNeighborsResponse> &responses);

    void checkResponseResult(const storage::cpp2::ResponseCommon &resp) const;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
