/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_PRODUCESEMISHORTESTPATHEXECUTOR_H_
#define EXECUTOR_QUERY_PRODUCESEMISHORTESTPATHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class ProduceSemiShortestPathExecutor final : public Executor {
public:
    ProduceSemiShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("ProduceSemiShortestPath", node, qctx) {}

    folly::Future<Status> execute() override;

    using CostPathMapType =
        std::unordered_map<Value, std::unordered_map<Value, std::pair<int64_t, std::vector<Path>>>>;
    using CostPathMapPtr =
        std::unordered_map<Value,
                           std::unordered_map<Value, std::pair<int64_t, std::vector<Path*>>>>;

private:
    void dstNotInHistory(const Edge& edge, CostPathMapType&);

    void dstInHistory(const Edge& edge, CostPathMapType&);

    void dstInCurrent(const Edge& edge, CostPathMapType&);

    void updateHistory(CostPathMapType&);

    std::vector<Path> createPaths(const std::vector<Path*>& paths, const Edge& edge);

private:
    // dst : {src : <cost, {Path*}>}
    CostPathMapPtr historyCostPathMap_;

    // std::unique_ptr<IWeight> weight_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_QUERY_PRODUCESEMISHORTESTPATHEXECUTOR_H_
