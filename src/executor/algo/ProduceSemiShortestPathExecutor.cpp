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
    for (auto& vid : starts_) {
        std::unordered_map<Value, std::pair<int64_t, std::shared_ptr<Path>>> temp = {
            {vid, {0, nullptr}}};
        costPathMap_[vid] = std::move(temp);
    }
}

void ProduceSemiShortestPathExecutor::updateJumpDstCostPath(const Edge& edge) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    if (costPathMap_.find(src) != costPathMap_.end() &&
        costPathMap_[src].find(dst) != costPathMap_[src].end()) {
        return;
    }
    VLOG(1) << "Update jump :" << src << " dst :" << dst << " edge : " << edge;
    std::shared_ptr<Path> path = std::make_shared<Path>();
    path->src = Vertex(src, {});
    path->steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));

    auto weight = 1;   //  weight = weight_->getWeight()
    costPathMap_[src].insert({{dst, std::make_pair(weight, std::move(path))}});
}

void ProduceSemiShortestPathExecutor::updateSrcDstCostPath(const Value& src, const Edge& edge) {
    auto jump = edge.src;
    auto dst = edge.dst;
    auto& costPath = costPathMap_[src];
    if (costPath.find(jump) == costPath.end()) {
        VLOG(1) << "No path from " << src << " to " << jump;
        return;
    }
    int64_t jumpCost = costPathMap_[src][jump].first + costPathMap_[jump][dst].first;
    auto iter = costPath.find(dst);
    if (iter != costPath.end() && jumpCost > costPathMap_[src][dst].first) {
        return;
    }
    VLOG(1) << "Update src :" << src << " jump: " << jump << " dst: " << dst << " edge: " << edge;

    std::shared_ptr<Path> jumpPath = std::make_shared<Path>();
    jumpPath->append(*costPathMap_[src][jump].second);
    jumpPath->append(*costPathMap_[jump][dst].second);

    costPath[dst] = std::make_pair(jumpCost, std::move(jumpPath));
}

folly::Future<Status> ProduceSemiShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    auto iter = ectx_->getResult(pssp->inputVar()).iter();
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << pssp->inputVar();
    DCHECK(!!iter);

    DataSet ds;
    ds.colNames = node()->colNames();

    for (auto& src : starts_) {
        for (; iter->valid(); iter->next()) {
            auto edgeVal = iter->getEdge();
            if (!edgeVal.isEdge()) {
                continue;
            }
            auto& edge = edgeVal.getEdge();
            // set (jump->dst)'s weight and path
            updateJumpDstCostPath(edge);
            // set (src->dst)'s weight and path
            if (edge.src != src) {
                updateSrcDstCostPath(src, edge);
            }
        }
    }

    for (auto& src : starts_) {
        for (auto& item : costPathMap_[src]) {
                auto& dst = item.first;
                auto& cost = item.second.first;
                auto path = item.second.second;
                Row row;
                row.values.emplace_back(dst);
                row.values.emplace_back(cost);
                row.values.emplace_back(*path);
                ds.rows.emplace_back(std::move(row));
        }
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}   // namespace graph
}   // namespace nebula
