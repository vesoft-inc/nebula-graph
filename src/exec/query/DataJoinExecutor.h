/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_DATAJOINEXECUTOR_H_
#define EXEC_QUERY_DATAJOINEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {
class DataJoinExecutor final : public Executor {
public:
    DataJoinExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DataJoinExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    std::unordered_map<List, const LogicalRow*> buildHashTable(
        const std::vector<Expression*>& hashKeys, Iterator* iter);

    void probe(const std::unordered_map<List, const LogicalRow*>& map,
                const std::vector<Expression*>& probeKeys, Iterator* iter,
                JoinIter* resultIter);
};
}  // namespace graph
}  // namespace nebula
#endif
