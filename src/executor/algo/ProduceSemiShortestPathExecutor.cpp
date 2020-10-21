/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceSemiShortestPathExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

std::vector<Path> ProduceSemiShortestPathExecutor::createPaths(
    const std::vector<const Path*>& paths,
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
            // startVid in currentCostPathMap[dst]
            auto newCost = srcPath.second.cost_ + weight;
            auto oldCost = currentCostPathMap[dst][srcPath.first].cost_;
            if (newCost > oldCost) {
                continue;
            } else if (newCost < oldCost) {
                // update (dst->startVid)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                currentCostPathMap[dst][srcPath.first].cost_ = newCost;
                currentCostPathMap[dst][srcPath.first].paths_.swap(newPaths);
            } else {
                // add (dst->startVid)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                for (auto& p : newPaths) {
                    // todo(jmq) maybe duplicate path
                    currentCostPathMap[dst][srcPath.first].paths_.emplace_back(std::move(p));
                }
            }
        } else {
            // startVid not in currentCostPathMap[dst], insert it
            auto newCost = srcPath.second.cost_ + weight;
            std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
            CostPaths temp(newCost, newPaths);
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
            auto cost = srcPath.second.cost_ + weight;

            std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
            std::unordered_map<Value, CostPaths> temp = {
                {srcPath.first, CostPaths(cost, newPaths)}};
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
                //  (dst->startVid)'s path not in history
                auto newCost = srcPath.second.cost_ + weight;
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                currentCostPathMap[dst].emplace(srcPath.first, CostPaths(newCost, newPaths));
            } else {
                //  (dst->startVid)'s path in history, compare cost
                auto newCost = srcPath.second.cost_ + weight;
                auto oldCost = historyCostPathMap_[dst][srcPath.first].cost_;
                if (newCost > oldCost) {
                    continue;
                } else {
                    // update (dst->startVid)'s path
                    std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                    currentCostPathMap[dst].emplace(srcPath.first, CostPaths(newCost, newPaths));
                }
            }
        }
    } else {
        // dst in hostory and in current
        dstInCurrent(edge, currentCostPathMap);
    }
}

void ProduceSemiShortestPathExecutor::updateHistory(const Value& dst,
                                                    const Value& src,
                                                    double cost,
                                                    Value& paths) {
    List pathList = paths.getList();
    std::vector<const Path*> tempPath;
    tempPath.reserve(pathList.size());
    for (auto& p : pathList.values) {
        tempPath.emplace_back(p.getPathPtr());
    }
    if (historyCostPathMap_.find(dst) == historyCostPathMap_.end()) {
        // insert path to history
        std::unordered_map<Value, CostPathsPtr> temp = {{src, CostPathsPtr(cost, tempPath)}};
        historyCostPathMap_.emplace(dst, std::move(temp));
    } else {
        if (historyCostPathMap_[dst].find(src) == historyCostPathMap_[dst].end()) {
            // startVid not in history ; insert it
            historyCostPathMap_[dst].emplace(src, CostPathsPtr(cost, tempPath));
        } else {
            // startVid in history; compare cost
            auto historyCost = historyCostPathMap_[dst][src].cost_;
            if (cost < historyCost) {
                historyCostPathMap_[dst][src].cost_ = cost;
                historyCostPathMap_[dst][src].paths_.swap(tempPath);
            } else if (cost == historyCost) {
                for (auto p : tempPath) {
                    historyCostPathMap_[dst][src].paths_.emplace_back(p);
                }
            } else {
                LOG(FATAL) << "current Cost : " << cost << " history Cost : " << historyCost;
            }
        }
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
            // src not in history, now src must be startVid
            Path path;
            // (todo) can't get dst's vertex
            path.src = Vertex(src, {});
            path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
            CostPaths costPaths(weight, {std::move(path)});
            if (currentCostPathMap.find(dst) != currentCostPathMap.end()) {
                currentCostPathMap[dst].emplace(src, std::move(costPaths));
            } else {
                std::unordered_map<Value, CostPaths> temp = {{src, std::move(costPaths)}};
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
    for (auto& dstPath : currentCostPathMap) {
        auto& dst = dstPath.first;
        for (auto& srcPath : dstPath.second) {
            auto cost = srcPath.second.cost_;
            List paths;
            paths.values.reserve(srcPath.second.paths_.size());
            for (auto & path : srcPath.second.paths_) {
                paths.values.emplace_back(std::move(path));
            }
            Row row;
            row.values.emplace_back(std::move(dst));
            row.values.emplace_back(std::move(cost));
            row.values.emplace_back(std::move(paths));
            ds.rows.emplace_back(std::move(row));

            // update (dst->startVid)'s paths to history
            updateHistory(dst, srcPath.first, cost, ds.rows.back().values.back());
        }
    }

    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}



}   // namespace graph
}   // namespace nebula
