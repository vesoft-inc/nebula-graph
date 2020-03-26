/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_GETNEIGHBORSEXECUTOR_H_
#define EXEC_GETNEIGHBORSEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GetNeighborsExecutor final : public SingleInputExecutor {
public:
    GetNeighborsExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input)
        : SingleInputExecutor("GetNeighborsExecutor", node, ectx, input) {}

private:
    folly::Future<Status> exec();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_GETNEIGHBORSEXECUTOR_H_
