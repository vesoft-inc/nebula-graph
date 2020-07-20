/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DataCollectExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataCollectExecutor::execute() {
    dumpLog();
    return doCollect().ensure([this] () {
        result_ = Value::kEmpty;
        colNames_.clear();
    });
}

folly::Future<Status> DataCollectExecutor::doCollect() {
    auto* dc = asNode<DataCollect>(node());
    colNames_ = dc->colNames();
    auto vars = dc->vars();
    switch (dc->collectKind()) {
        case DataCollect::CollectKind::kSubgraph: {
            NG_RETURN_IF_ERROR(collectSubgraph(vars));
            break;
        }
        case DataCollect::CollectKind::kRowBasedMove: {
            NG_RETURN_IF_ERROR(rowBasedMove(vars));
            break;
        }
        default:
            LOG(FATAL) << "Unknown data collect type: " << static_cast<int64_t>(dc->collectKind());
    }
    ResultBuilder builder;
    builder.value(Value(std::move(result_))).iter(Iterator::Kind::kSequential);
    return finish(builder.finish());
}

Status DataCollectExecutor::collectSubgraph(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        for (auto& result : hist) {
            Row row;
            auto iter = result.iter();
            if (iter->isGetNeighborsIter()) {
                auto* gnIter = static_cast<GetNeighborsIter*>(iter.get());
                row.values.emplace_back(gnIter->getVertices());
                row.values.emplace_back(gnIter->getEdges());
                ds.rows.emplace_back(std::move(row));
            } else {
                return Status::Error("Iterator should be kind of GetNeighborIter.");
            }
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::rowBasedMove(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());
    for (auto& var : vars) {
        auto& result = ectx_->getResult(var);
        auto iter = result.iter();
        if (iter->isSequentialIter()) {
            auto* seqIter = static_cast<SequentialIter*>(iter.get());
            for (; seqIter->valid(); seqIter->next()) {
                ds.rows.emplace_back(seqIter->moveRow());
            }
        } else {
            return Status::Error("Iterator should be kind of SequentialIter.");
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
