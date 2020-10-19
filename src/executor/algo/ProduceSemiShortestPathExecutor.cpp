/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceSemiShortestPathExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

void ProduceSemiShortestPathExecutor::init() {
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    starts_ = pssp->getStartsVid();
    //  weight_ = pssp->getWeight();
}

folly::Future<Status> ProduceSemiShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    auto iter = ectx_->getResult(pssp->inputVar()).iter();
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << pssp->inputVar();
    DCHECK(!!iter);

    for (; iter->valid(); iter->next()) {
        auto edgeVal = iter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto& src = edge.src;
        auto& dst = edge.dst;
        auto weight = 1;
        if (costPathMap_.find(dst) == costPathMap_.end()) {
            costPathMap_[dst].emplace_back();

            if (costPathMap_.find(src) == costPathMap_.end()) {
                Path* start = new Path();
                start->src = Vertex(src, {});
                start->steps.emplace_back(
                    Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
                costPathMap_[dst].back().first = weight;
                costPathMap_[dst].back().second = start;
            } else {
                for (auto& item : costPathMap_[src]) {
                    auto newCost = weight + item.first;
                    Path* path = new Path(*item.second);
                    path->steps.emplace_back(
                        Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
                    costPathMap_[dst].emplace_back(std::make_pair(newCost, path));
                }
            }
        } else {
            if (costPathMap_.find(src) == costPathMap_.end()) {
                Path* start = new Path();
                start->src = Vertex(src, {});
                start->steps.emplace_back(
                    Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
                costPathMap_[dst].back().first = weight;
                costPathMap_[dst].back().second = start;
            } else {
                auto oldCost = costPathMap_[dst].front().first;
                auto newCost = costPathMap_[src].front().first + weight;
                if (newCost > oldCost) {
                    continue;
                }
                costPathMap_[dst].clear();
                for (auto& item : costPathMap_[src]) {
                    Path* newPath = new Path(*item.second);
                    newPath->steps.emplace_back(
                        Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
                    costPathMap_[dst].emplace_back(std::make_pair(newCost, newPath));
                }
            }
        }
    }

    DataSet ds;
    ds.colNames = node()->colNames();
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
