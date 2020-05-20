/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_
#define EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_

#include "exec/Executor.h"
#include "expression/Expression.h"

namespace nebula {
namespace graph {

class DeleteVerticesExecutor final : public SingleInputExecutor {
public:
    DeleteVerticesExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input)
            : SingleInputExecutor("DeleteVerticesExecutor", node, ectx, input) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> deleteVertices();
private:
    GraphSpaceID                        space_;
    std::vector<VertexID>               vertices_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_
