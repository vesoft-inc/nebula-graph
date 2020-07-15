/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DataJoinExecutor.h"

#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "context/Iterator.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataJoinExecutor::execute() {
    dumpLog();
    auto* dataJoin = asNode<DataJoin>(node());

    auto lhsIter = ectx_->getResult(dataJoin->vars().first).iter();
    DCHECK(!!lhsIter);
    auto map = buildHashTable(dataJoin->hashKeys(), lhsIter.get());

    auto rhsIter = ectx_->getResult(dataJoin->vars().second).iter();
    DCHECK(!!rhsIter);
    auto resultIter = std::make_unique<JoinIter>();
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    if (!map.empty()) {
        probe(map, dataJoin->probeKeys(), rhsIter.get(), resultIter.get());
    }
    return finish(ResultBuilder().iter(std::move(std::move(resultIter))).finish());
}

std::unordered_map<List, const LogicalRow*> DataJoinExecutor::buildHashTable(
    const std::vector<Expression*>& hashKeys, Iterator* iter) {
    QueryExpressionContext ctx(ectx_, iter);
    std::unordered_map<List, const LogicalRow*> map;
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        map.emplace(std::move(list), iter->row());
    }

    return map;
}

void DataJoinExecutor::probe(
    const std::unordered_map<List, const LogicalRow*>& map,
    const std::vector<Expression*>& probeKeys, Iterator* iter,
    JoinIter* resultIter) {
    QueryExpressionContext ctx(ectx_, iter);

    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        auto found = map.find(list);
        if (found != map.end()) {
            auto lSegs = found->second->segments();
            std::vector<const Row*> values;
            values.insert(values.end(),
                          std::make_move_iterator(lSegs.begin()),
                          std::make_move_iterator(lSegs.end()));
            auto rSegs = iter->row()->segments();
            values.insert(values.end(),
                          std::make_move_iterator(rSegs.begin()),
                          std::make_move_iterator(rSegs.end()));
            size_t size = found->second->size() + iter->row()->size();
            JoinIter::LogicalRowJoin row(std::move(values), size,
                                         &resultIter->getColIdxIndices());
            resultIter->addRow(std::move(row));
        }
    }
}
}  // namespace graph
}  // namespace nebula
