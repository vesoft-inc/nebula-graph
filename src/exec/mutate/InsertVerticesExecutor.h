/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_INSERTVERTICESEXECUTOR_H_
#define EXEC_MUTATE_INSERTVERTICESEXECUTOR_H_

#include "exec/StorageExecutor.h"

namespace nebula {
namespace graph {

class InsertVerticesExecutor final : public StorageExecutor {
public:
    InsertVerticesExecutor(const PlanNode *node, QueryContext *qctx)
        : StorageExecutor("InsertVerticesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> insertVertices();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_INSERTVERTICESEXECUTOR_H_
