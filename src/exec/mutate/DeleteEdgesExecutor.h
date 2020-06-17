/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_DELETEEDGEEXECUTOR_H_
#define EXEC_MUTATE_DELETEEDGEEXECUTOR_H_

#include "exec/Executor.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "planner/Mutate.h"

namespace nebula {
namespace graph {

class DeleteEdgesExecutor final : public Executor {
public:
    DeleteEdgesExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("DeleteEdgesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> deleteEdges();
    Status prepareEdgeKeys(const EdgeType edgeType, const EdgeKeys *edgeKeys);

private:
    std::vector<storage::cpp2::EdgeKey>              edgeKeys_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_DELETEEDGEEXECUTOR_H_
