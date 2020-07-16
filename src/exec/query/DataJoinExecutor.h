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
 void buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter);

 void probe(const std::vector<Expression*>& probeKeys, Iterator* iter,
            JoinIter* resultIter);

private:
    size_t                                                        bucketSize_{0};
    bool                                                          exchange_{false};
    std::vector<std::list<std::pair<List, const LogicalRow*>>>    hashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
