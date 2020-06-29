/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_DATACOLLECTEXECUTOR_H_
#define EXEC_QUERY_DATACOLLECTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {
class DataCollectExecutor final : public Executor {
public:
    DataCollectExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DataCollectExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    Status collectSubgraph(const std::vector<std::string>& vars);

    std::vector<std::string>    colNames_;
    Value                       result_;
};
}  // namespace graph
}  // namespace nebula
#endif
