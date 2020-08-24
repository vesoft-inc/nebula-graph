/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_PARTEXECUTOR_H_
#define EXECUTOR_ADMIN_PARTEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class ShowPartsExecutor final : public Executor {
public:
    ShowPartsExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowPartsExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_PARTEXECUTOR_H_
