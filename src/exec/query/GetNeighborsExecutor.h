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
#include "util/StopWatch.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public SingleInputExecutor {
public:
    GetNeighborsExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input)
        : SingleInputExecutor("GetNeighborsExecutor", node, ectx, input), watch_(elapsedTime_) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getNeighbors();

    Status handleResponse(
        const std::vector<nebula::storage::cpp2::GetNeighborsResponse> &responses);

    Status collectVertexTags(const std::vector<storage::cpp2::VertexProp> &schema,
                             const std::vector<nebula::Value> &resp,
                             std::vector<nebula::Tag> *tags) const;

    StopWatch watch_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETNEIGHBORSEXECUTOR_H_
