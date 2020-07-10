/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DataJoinExecutor.h"

#include "planner/Query.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataJoinExecutor::execute() {
    dumpLog();
    auto* dataJoin = asNode<DataJoin>(node());

    auto map = buildHashTable(dataJoin->hashKeys(), dataJoin->lhsCols()->columns(),
                              dataJoin->vars().first);
    DataSet result;
    if (!map.empty()) {
        result = probe(map, dataJoin->probeKeys(), dataJoin->rhsCols()->columns(),
                        dataJoin->vars().second);
    }
    result.colNames = dataJoin->colNames();
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

std::unordered_map<List, std::vector<Value>> DataJoinExecutor::buildHashTable(
    const std::vector<Expression*>& hashKeys,
    const std::vector<YieldColumn*>& cols,
    const std::string& var) {
    auto iter = ectx_->getResult(var).iter();
    DCHECK(!!iter);
    ExpressionContextImpl ctx(ectx_, iter.get());
    std::unordered_map<List, std::vector<Value>> map;
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        std::vector<Value> values;
        values.reserve(cols.size());
        for (auto& col : cols) {
            Value val = col->expr()->eval(ctx);
            values.emplace_back(std::move(val));
        }

        map.emplace(std::move(list), std::move(values));
    }

    return map;
}

DataSet DataJoinExecutor::probe(
    const std::unordered_map<List, std::vector<Value>>& map,
    const std::vector<Expression*>& probeKeys,
    const std::vector<YieldColumn*>& cols,
    const std::string& var) {
    auto iter = ectx_->getResult(var).iter();
    DCHECK(!!iter);
    ExpressionContextImpl ctx(ectx_, iter.get());
    DataSet result;

    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        auto found = map.find(list);
        if (found != map.end()) {
            Row row;
            row.values.reserve(found->second.size());
            row.values.insert(row.values.begin(), found->second.begin(), found->second.end());
            row.values.reserve(cols.size());
            for (auto& col : cols) {
                Value val = col->expr()->eval(ctx);
                row.values.emplace_back(std::move(val));
            }
            result.rows.emplace_back(std::move(row));
        }
    }

    return result;
}
}  // namespace graph
}  // namespace nebula
