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
    DataSet ds;
    ds.colNames = subgraph->colNames();

    auto isOneMoreStep = ectx_->getValue(subgraph->isOneMoreStep());
    DCHECK(isOneMoreStep.isBool());
    if (isOneMoreStep.getBool()) {
        oneMoreStep();
        return finish(ResultBuilder().value(Value(std::move(ds))).finish());
    }

    auto lastStep = ectx_->getValue(subgraph->lastStep());
    DCHECK(lastStep.isBool());

    VLOG(1) << "lastStep : " << lastStep;
    VLOG(1) << "input: " << subgraph->inputVar() << " output: " << node()->outputVar();
    auto iter = ectx_->getResult(subgraph->inputVar()).iter();
    DCHECK(iter->isGetNeighborsIter());
    DCHECK(!!iter);
    ds.rows.reserve(iter->size());
    if (lastStep.getBool()) {
        std::unordered_set<std::string> visitedVid;
        for (; iter->valid(); iter->next()) {
            const auto& dst = iter->getEdgeProp("*", nebula::kDst);
            if (visitedVid.emplace(dst.toString().c_str()).second) {
                Row row;
                row.values.emplace_back(std::move(dst));
                ds.rows.emplace_back(std::move(row));
            }
            const auto& vid = iter->getColumn(nebula::kVid);
            visitedVid.emplace(vid.toString().c_str());
        }
        historyVids_.insert(std::make_move_iterator(visitedVid.begin()),
                            std::make_move_iterator(visitedVid.end()));
        VLOG(1) << "next step vid is : " << ds;
        return finish(ResultBuilder().value(Value(std::move(ds))).finish());
    }

    for (; iter->valid(); iter->next()) {
        const auto& dst = iter->getEdgeProp("*", nebula::kDst);
        if (historyVids_.emplace(dst.toString().c_str()).second) {
            Row row;
            row.values.emplace_back(std::move(dst));
            ds.rows.emplace_back(std::move(row));
        }
        const auto& vid = iter->getColumn(nebula::kVid);
        historyVids_.emplace(vid.toString().c_str());
    }
    VLOG(1) << "next step vid is : " << ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

void SubgraphExecutor::oneMoreStep() {
    auto* subgraph = asNode<Subgraph>(node());
    auto input = ectx_->getValue(subgraph->oneMoreStepInput()).getStr();
    auto output = subgraph->oneMoreStepOutput();
    VLOG(1) << "OneMoreStep Input: " << input << " Output: " << output;
    auto iter = ectx_->getResult(input).iter();
    DCHECK(iter->isGetNeighborsIter());
    DCHECK(!!iter);

    ResultBuilder builder;
    builder.value(iter->valuePtr());
    while (iter->valid()) {
        const auto& dstVid = iter->getEdgeProp("*", nebula::kDst).toString().c_str();
        if (historyVids_.find(dstVid) == historyVids_.end()) {
            iter->unstableErase();
        } else {
            iter->next();
        }
    }
    iter->reset();
    builder.iter(std::move(iter));
    ectx_->setResult(output, builder.finish());
}

}   // namespace graph
}   // namespace nebula
