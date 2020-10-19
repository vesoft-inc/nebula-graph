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
        : Executor("ProduceSemiShortestPath", node, qctx) {
            init();
        }

    folly::Future<Status> execute() override;

    void init();

private:
    void updateSrcDstCostPath(const Value& src, const Edge& edge);
    void updateJumpDstCostPath(const Edge& edge);

private:
    // origin vids
    std::vector<Value> starts_;
    //  src : {dst : {cost, {edge}}}
    std::unordered_map<Value, std::unordered_map<Value, std::pair<int64_t, std::shared_ptr<Path>>>>
        costPathMap_;

    // std::unique_ptr<IWeight> weight_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_QUERY_PRODUCESEMISHORTESTPATHEXECUTOR_H_
