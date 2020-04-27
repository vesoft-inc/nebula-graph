/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
#define EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_

#include <vector>

#include "base/StatusOr.h"
#include "datatypes/Value.h"
#include "datatypes/Vertex.h"
#include "exec/Executor.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public SingleInputExecutor {
public:
    GetNeighborsExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input)
        : SingleInputExecutor("GetNeighborsExecutor", node, ectx, input) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getNeighbors();

    Status handleResponse(const std::vector<storage::cpp2::GetNeighborsResponse> &responses);

    Status collectVertexTags(const std::vector<std::string> &schema,
                             const std::vector<Value> &resp,
                             std::vector<Tag> *tags) const;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
