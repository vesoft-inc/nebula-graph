/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_SCHEMA_INSERTVERTICESEXECUTOR_H_
#define EXEC_SCHEMA_INSERTVERTICESEXECUTOR_H_

#include "exec/Executor.h"
#include "util/StopWatch.h"

namespace nebula {
namespace graph {

class InsertVericesExecutor final : public Executor {
public:
    InsertVericesExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("InsertVericesExecutor", node, ectx), watch_(elapsedTime_) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> createSpace();

    StopWatch watch_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_SCHEMA_INSERTVERTICESEXECUTOR_H_
