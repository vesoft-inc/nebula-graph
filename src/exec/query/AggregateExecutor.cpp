/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/AggregateExecutor.h"

#include "common/datatypes/List.h"
#include "common/function/AggregateFunction.h"

#include "context/ExpressionContextImpl.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> AggregateExecutor::execute() {
    dumpLog();
    auto* agg = asNode<Aggregate>(node());
    auto groupKeys = agg->groupKeys();
    auto groupItems = agg->groupItems();
    auto iter = ectx_->getResult(agg->inputVar()).iter();
    DCHECK(!!iter);
    ExpressionContextImpl ctx(ectx_, iter.get());

    std::unordered_map<List, std::vector<std::unique_ptr<AggFun>>> result;
    for (; iter->valid(); iter->next()) {
        List list;
        for (auto& key : groupKeys) {
            list.values.emplace_back(key->eval(ctx));
        }

        auto it = result.find(list);
        if (it == result.end()) {
            std::vector<std::unique_ptr<AggFun>> funs;
            for (auto& item : groupItems) {
                auto fun = AggFun::aggFunMap_[item.second]();
                auto& v = item.first->eval(ctx);
                fun->apply(v);
                funs.emplace_back(std::move(fun));
            }
            result.emplace(std::make_pair(std::move(list), std::move(funs)));
        } else {
            DCHECK_EQ(it->second.size(), groupItems.size());
            for (size_t i = 0; i < groupItems.size(); ++i) {
                auto& v = groupItems[i].first->eval(ctx);
                it->second[i]->apply(v);
            }
        }
    }

    DataSet ds;
    ds.colNames = std::move(agg->colNames());
    ds.rows.reserve(result.size());
    for (auto& kv : result) {
        Row row;
        for (auto& f : kv.second) {
            row.columns.emplace_back(f->getResult());
        }
        ds.rows.emplace_back(std::move(row));
    }
    return finish(ExecResult::buildSequential(
                Value(std::move(ds)), State(State::Stat::kSuccess, "")));
}

}   // namespace graph
}   // namespace nebula
