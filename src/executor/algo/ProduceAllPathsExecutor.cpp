/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceAllPathsExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> ProduceAllPathsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* allPaths = asNode<ProduceAllPaths>(node());
    auto iter = ectx_->getResult(allPaths->inputVar()).iter();
    DCHECK(!!iter);

    DataSet ds;
    ds.colNames = node()->colNames();
    Interims interims;

    for (; iter->valid(); iter->next()) {
        auto edgeVal = iter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto histPaths = historyPaths_.find(edge.src);
        if (histPaths == historyPaths_.end()) {
            createPaths(edge, interims);
        } else {
            buildPaths(histPaths->second, edge, interims);
        }
    }

    for (auto& interim : interims) {
        Row row;
        row.values.emplace_back(std::move(interim.dst));
        row.values.emplace_back(std::move(interim.path));
        ds.rows.emplace_back(std::move(row));
        auto& dst = ds.rows.back().values.front();
        historyPaths_[dst].emplace_back(&ds.rows.back().values.back().getPath());
    }
    count_++;
    return Status::OK();
}

void ProduceAllPathsExecutor::createPaths(const Edge& edge, Interims& interims) {
    Path path;
    path.src = Vertex(edge.src, {});
    path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
    interims.emplace_back(edge.dst, std::move(path));
}

void ProduceAllPathsExecutor::buildPaths(const std::vector<const Path*>& history,
                                         const Edge& edge,
                                         Interims& interims) {
    for (auto* histPath : history) {
        if (histPath->steps.size() < count_) {
            continue;
        } else {
            Path path = *histPath;
            path.steps.emplace_back(
                Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
            interims.emplace_back(edge.dst, std::move(path));
        }
    }
}
}  // namespace graph
}  // namespace nebula
