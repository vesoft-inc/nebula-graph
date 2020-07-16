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
    auto rhsIter = ectx_->getResult(dataJoin->vars().second).iter();
    DCHECK(!!rhsIter);
    auto resultIter = std::make_unique<JoinIter>();
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    bucketSize_ =
        lhsIter->size() > rhsIter->size() ? rhsIter->size() : lhsIter->size();
    hashTable_.resize(bucketSize_);

    if (!(lhsIter->size() == 0 || rhsIter->size() == 0)) {
        if (lhsIter->size() < rhsIter->size()) {
            buildHashTable(dataJoin->hashKeys(), lhsIter.get());
            probe(dataJoin->probeKeys(), rhsIter.get(), resultIter.get());
        } else {
            exchange_ = true;
            buildHashTable(dataJoin->probeKeys(), rhsIter.get());
            probe(dataJoin->hashKeys(), lhsIter.get(), resultIter.get());
        }
    }
    return finish(ResultBuilder().iter(std::move(std::move(resultIter))).finish());
}

void DataJoinExecutor::buildHashTable(const std::vector<Expression*>& hashKeys,
                                      Iterator* iter) {
    QueryExpressionContext ctx(ectx_, iter);
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        auto hash = std::hash<List>()(list);
        auto bucket = hash % bucketSize_;
        hashTable_[bucket].emplace_back(std::make_pair(std::move(list), iter->row()));
    }
}

void DataJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                             Iterator* iter, JoinIter* resultIter) {
    QueryExpressionContext ctx(ectx_, iter);

    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        auto hash = std::hash<List>()(list);
        auto bucket = hash % bucketSize_;
        auto rows = hashTable_[bucket];
        for (auto& row : rows) {
            if (row.first == list) {
                std::vector<const Row*> values;
                auto lSegs = row.second->segments();
                auto rSegs = iter->row()->segments();
                if (exchange_) {
                    values.insert(values.end(),
                                std::make_move_iterator(rSegs.begin()),
                                std::make_move_iterator(rSegs.end()));
                    values.insert(values.end(),
                                std::make_move_iterator(lSegs.begin()),
                                std::make_move_iterator(lSegs.end()));
                } else {
                    values.insert(values.end(),
                                std::make_move_iterator(lSegs.begin()),
                                std::make_move_iterator(lSegs.end()));
                    values.insert(values.end(),
                                std::make_move_iterator(rSegs.begin()),
                                std::make_move_iterator(rSegs.end()));
                }
                size_t size = row.second->size() + iter->row()->size();
                JoinIter::LogicalRowJoin newRow(std::move(values), size,
                                            &resultIter->getColIdxIndices());
                resultIter->addRow(std::move(newRow));
            }
        }
    }
}
}  // namespace graph
}  // namespace nebula
