/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DataJoinExecutor.h"

#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "context/Iterator.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataJoinExecutor::execute() {
    return doInnerJoin();
}

Status DataJoinExecutor::close() {
    exchange_ = false;
    hashTable_.clear();
    return Executor::close();
}

folly::Future<Status> DataJoinExecutor::doInnerJoin() {
    SCOPED_TIMER(&execTime_);

    auto* dataJoin = asNode<DataJoin>(node());
    auto lhsIter = ectx_
                       ->getVersionedResult(dataJoin->leftVar().first,
                                            dataJoin->leftVar().second)
                       .iter();
    DCHECK(!!lhsIter);
    if (lhsIter->isGetNeighborsIter() || lhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }
    auto rhsIter = ectx_
                       ->getVersionedResult(dataJoin->rightVar().first,
                                            dataJoin->rightVar().second)
                       .iter();
    DCHECK(!!rhsIter);
    if (lhsIter->isGetNeighborsIter() || lhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }


    DataSet result;
    if (!(lhsIter->empty() || rhsIter->empty())) {
        if (lhsIter->size() < rhsIter->size()) {
            buildHashTable(dataJoin->hashKeys(), lhsIter.get());
            result = probe(dataJoin->probeKeys(), rhsIter.get());
        } else {
            exchange_ = true;
            buildHashTable(dataJoin->probeKeys(), rhsIter.get());
            result = probe(dataJoin->hashKeys(), lhsIter.get());
        }
    }
    result.colNames = dataJoin->colNames();
    VLOG(1) << result;
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

void DataJoinExecutor::buildHashTable(const std::vector<Expression*>& hashKeys,
                                      Iterator* iter) {
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx(iter));
            list.values.emplace_back(std::move(val));
        }

        auto& vals = hashTable_[list];
        vals.emplace_back(iter->row());
    }
}

DataSet DataJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                             Iterator* probeIter) {
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx(probeIter));
            list.values.emplace_back(std::move(val));
        }

        const auto& range = hashTable_.find(list);
        if (range == hashTable_.end()) {
            continue;
        }
        for (auto* row : range->second) {
            auto& lRow = *row;
            auto& rRow = *probeIter->row();
            VLOG(1) << lRow << rRow;
            Row newRow;
            newRow.reserve(lRow.size() + rRow.size());
            auto& values = newRow.values;
            if (exchange_) {
                values.insert(values.end(), rRow.values.begin(), rRow.values.end());
                values.insert(values.end(), lRow.values.begin(), lRow.values.end());
            } else {
                values.insert(values.end(), lRow.values.begin(), lRow.values.end());
                values.insert(values.end(), rRow.values.begin(), rRow.values.end());
            }
            VLOG(1) << "Row: " << newRow;
            ds.rows.emplace_back(std::move(newRow));
        }
    }
    return ds;
}
}  // namespace graph
}  // namespace nebula
