/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_CONJUNCTPATHEXECUTOR_H_
#define EXECUTOR_QUERY_CONJUNCTPATHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class ConjunctPathExecutor final : public Executor {
public:
    ConjunctPathExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("ConjunctPathExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_QUERY_CONJUNCTPATHEXECUTOR_H_
