/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/SubgraphExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubgraphExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* subgraph = asNode<Subgraph>(node());
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << subgraph->inputVar();
    DataSet ds;
    ds.colNames = subgraph->colNames();

    auto isLastStepVar = subgraph->isLastStep();
    auto isLastStep = ectx_->getValue(isLastStepVar);
    DCHECK(isLastStep.isBool());
    if (isLastStep.getBool()) {
        oneMoreStep();
        return finish(ResultBuilder().value(Value(std::move(ds))).finish());
    }

    auto iter = ectx_->getResult(subgraph->inputVar()).iter();
    DCHECK(iter->isGetNeighborsIter());
    DCHECK(!!iter);
    ds.rows.reserve(iter->size());
    for (; iter->valid(); iter->next()) {
        auto vid = iter->getColumn(nebula::kVid);
        if (visitedVids_.emplace(vid.toString().c_str()).second) {
            Row row;
            row.values.emplace_back(std::move(vid));
            ds.rows.emplace_back(std::move(row));
        }
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

void SubgraphExecutor::oneMoreStep() {
    auto* subgraph = asNode<Subgraph>(node());
    auto lastStepVar = subgraph->lastStepVar();
    auto iter = ectx_->getResult(subgraph->inputVar()).iter();
    DCHECK(iter->isGetNeighborsIter());
    DCHECK(!!iter);

    ResultBuilder builder;
    builder.value(iter->valuePtr());
    while (iter->valid()) {
        // get dstVid
        auto dstVid = iter->getEdgeProp("*", nebula::kDst).toString().c_str();
        if (visitedVids_.find(dstVid) == visitedVids_.end()) {
            iter->unstableErase();
        } else {
            iter->next();
        }
    }
    iter->reset();
    builder.iter(std::move(iter));
    ectx_->setResult(lastStepVar, builder.finish());
}

}   // namespace graph
}   // namespace nebula
