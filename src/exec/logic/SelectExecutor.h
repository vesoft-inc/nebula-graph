/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_SELECTEXECUTOR_H_
#define EXEC_QUERY_SELECTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class SelectExecutor final : public Executor {
public:
    SelectExecutor(const PlanNode* node, ExecutionContext* ectx, Executor* then, Executor* els);

    folly::Future<Status> execute() override;

    Executor* thenBody() const {
        return then_;
    }

    Executor* elseBody() const {
        return else_;
    }

private:
    Executor* then_;
    Executor* else_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_SELECTEXECUTOR_H_
