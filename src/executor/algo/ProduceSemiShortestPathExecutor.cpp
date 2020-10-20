/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceSemiShortestPathExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

std::vector<Path> ProduceSemiShortestPathExecutor::createPaths(const std::vector<Path*>& paths,
                                                               const Edge& edge) {
    std::vector<Path> newPaths;
    newPaths.reserve(paths.size());
    for (auto p : paths) {
        Path path = *p;
        path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
        newPaths.emplace_back(std::move(path));
    }
    return newPaths;
}

void ProduceSemiShortestPathExecutor::dstInCurrent(const Edge& edge,
                                                   CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];

    for (auto& srcPath : srcPaths) {
        if (currentCostPathMap[dst].find(srcPath.first) != currentCostPathMap[dst].end()) {
            // src in currentCostPathMap[dst]
            auto newCost = srcPath.second.first + weight;
            auto oldCost = currentCostPathMap[dst][srcPath.first].first;
            if (newCost > oldCost) {
                continue;
            } else if (newCost < oldCost) {
                // update (dst->src)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
                currentCostPathMap[dst][srcPath.first].first = newCost;
                currentCostPathMap[dst][srcPath.first].second.swap(newPaths);
            } else {
                // add (dst->src)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
                for (auto& p : newPaths) {
                    // todo(jmq) maybe duplicate path
                    currentCostPathMap[dst][srcPath.first].second.emplace_back(std::move(p));
                }
            }
        } else {
            // src not in currentCostPathMap[dst], insert it
            auto newCost = srcPath.second.first + weight;
            std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
            std::pair<int64_t, std::vector<Path>> temp =
                std::make_pair(newCost, std::move(newPaths));
            currentCostPathMap[dst].emplace(srcPath.first, std::move(temp));
        }
    }
}

void ProduceSemiShortestPathExecutor::dstNotInHistory(const Edge& edge,
                                                      CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];
    if (currentCostPathMap.find(dst) == currentCostPathMap.end()) {
        //  dst not in history and not in current
        for (auto& srcPath : srcPaths) {
            auto cost = srcPath.second.first + weight;

            std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
            std::unordered_map<Value, std::pair<int64_t, std::vector<Path>>> temp = {
                {srcPath.first, {cost, std::move(newPaths)}}};
            currentCostPathMap.emplace(dst, std::move(temp));
        }
    } else {
        // dst not in history but in current
        dstInCurrent(edge, currentCostPathMap);
    }
}

void ProduceSemiShortestPathExecutor::dstInHistory(const Edge& edge,
                                                   CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];

    if (currentCostPathMap.find(dst) == currentCostPathMap.end()) {
        // dst not in current but in history
        for (auto& srcPath : srcPaths) {
            if (historyCostPathMap_[dst].find(srcPath.first) == historyCostPathMap_[dst].end()) {
                //  (dst->src)'s path not in history
                auto newCost = srcPath.second.first + weight;
                std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
                std::pair<int64_t, std::vector<Path>> temp =
                    std::make_pair(newCost, std::move(newPaths));
                currentCostPathMap[dst].emplace(srcPath.first, std::move(temp));
            } else {
                //  (dst->src)'s path in history, compare cost
                auto newCost = srcPath.second.first + weight;
                auto oldCost = historyCostPathMap_[dst][srcPath.first].first;
                if (newCost > oldCost) {
                    continue;
                } else {
                    // update (dst->src)'s path
                    std::vector<Path> newPaths = createPaths(srcPath.second.second, edge);
                    std::pair<int64_t, std::vector<Path>> temp =
                        std::make_pair(newCost, std::move(newPaths));
                    currentCostPathMap[dst].emplace(srcPath.first, std::move(temp));
                }
            }
        }
    } else {
        // dst in hostory and in current
        dstInCurrent(edge, currentCostPathMap);
    }
}

folly::Future<Status> ProduceSemiShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    auto iter = ectx_->getResult(pssp->inputVar()).iter();
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << pssp->inputVar();
    DCHECK(!!iter);

    CostPathMapType currentCostPathMap;

    for (; iter->valid(); iter->next()) {
        auto edgeVal = iter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto& src = edge.src;
        auto& dst = edge.dst;
        auto weight = 1;

        if (historyCostPathMap_.find(src) == historyCostPathMap_.end()) {
            // src not in history, start vid
            Path path;
            path.src = Vertex(src, {});
            path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
            if (currentCostPathMap.find(dst) != currentCostPathMap.end()) {
                std::pair<int64_t, std::vector<Path>> temp = {weight, {std::move(path)}};
                currentCostPathMap[dst].emplace(src, std::move(temp));
            } else {
                std::unordered_map<Value, std::pair<int64_t, std::vector<Path>>> temp = {
                    {src, {weight, {std::move(path)}}}};
                currentCostPathMap.emplace(dst, std::move(temp));
            }
        } else {
            if (historyCostPathMap_.find(dst) == historyCostPathMap_.end()) {
                dstNotInHistory(edge, currentCostPathMap);
            } else {
                dstInHistory(edge, currentCostPathMap);
            }
        }
    }

    DataSet ds;
    ds.colNames = node()->colNames();
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
