/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceSemiShortestPathExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

void ProduceSemiShortestPathExecutor::setStartsVid() {
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    starts_ = pssp->getStartsVid();
    for (auto& vid : starts_) {
        costPathMap_.emplace(vid,
                             std::unordered_map<Value, std::pair<int64_t, std::shared_ptr<Path>>>{
                                 {vid, {0, nullptr}}});
    }
}

void ProduceSemiShortestPathExecutor::updateJumpDstCostPath(const Edge& edge) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    VLOG(1) << "Update jump :" << src << " dst :" << dst << " edge : " << edge;
    if (costPathMap_.find(src) == costPathMap_.end() ||
        costPathMap_[src].find(dst) == costPathMap_[src].end()) {
        std::shared_ptr<Path> path = std::make_shared<Path>();
        path->src = Vertex(src, {});
        path->steps.emplace_back(
            Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));

        // weight_->getWeight()
        costPathMap_.emplace(
            src,
            std::unordered_map<Value, std::pair<int64_t, std::shared_ptr<Path>>>{{dst, {1, path}}});
    }
}

void ProduceSemiShortestPathExecutor::updateSrcDstCostPath(const Value& src, const Edge& edge) {
    auto jump = edge.src;
    auto dst = edge.dst;
    VLOG(1) << "src :" << src << " jump: " << jump << " dst: " << dst << " edge: " << edge;
    auto& costPath = costPathMap_[src];
    int64_t jumpCost = costPathMap_[src][jump].first + costPathMap_[jump][dst].first;

    if (costPath.find(dst) == costPath.end() || jumpCost < costPathMap_[src][dst].first) {
        std::shared_ptr<Path> path = costPathMap_[src][jump].second;
        path->append(*(costPathMap_[jump][dst].second));
        costPathMap_.insert(
            std::make_pair(src,
                           std::unordered_map<Value, std::pair<int64_t, std::shared_ptr<Path>>>(
                               {{dst, {jumpCost, path}}})));
    }
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
    std::multimap<Value, Value> interim;

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

    // (todo) collect result
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}   // namespace graph
}   // namespace nebula
