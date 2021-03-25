/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/StorageAccessExecutor.h"

#include "common/interface/gen-cpp2/meta_types.h"
#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

namespace internal {

template <typename ValueType>
struct Vid;

template <>
struct Vid<int64_t> {
    static int64_t value(const Value &v) {
        return v.getInt();
    }
};

template <>
struct Vid<std::string> {
    static std::string value(const Value &v) {
        return v.getStr();
    }
};

}   // namespace internal

bool StorageAccessExecutor::isIntVidType() const {
    const auto &space = qctx()->rctx()->session()->space();
    return space.spaceDesc.vid_type.type == meta::cpp2::PropertyType::INT64;
}

template <typename ValueType>
DataSet StorageAccessExecutor::buildRequestDataSet(Iterator *iter, Expression *expr, bool dedup) {
    DCHECK(iter && expr) << "iter=" << iter << ", expr=" << expr;
    nebula::DataSet vertices({kVid});
    vertices.rows.reserve(iter->size());

    std::unordered_set<ValueType> uniqueSet;
    uniqueSet.reserve(iter->size());

    const auto &spaceInfo = qctx()->rctx()->session()->space();
    auto vidType = spaceInfo.spaceDesc.vid_type;

    QueryExpressionContext exprCtx(qctx()->ectx());

    for (; iter->valid(); iter->next()) {
        auto vid = expr->eval(exprCtx(iter));
        if (!SchemaUtil::isValidVid(vid, vidType)) {
            LOG(WARNING) << "Mismatched vid type: " << vid.type()
                         << ", space vid type: " << SchemaUtil::typeToString(vidType);
            continue;
        }
        if (dedup && !uniqueSet.emplace(internal::Vid<ValueType>::value(vid)).second) {
            continue;
        }
        vertices.emplace_back(Row({std::move(vid)}));
    }
    return vertices;
}

DataSet StorageAccessExecutor::buildRequestDataSetByVidType(Iterator *iter,
                                                            Expression *expr,
                                                            bool dedup) {
    if (isIntVidType()) {
        return buildRequestDataSet<int64_t>(iter, expr, dedup);
    }
    return buildRequestDataSet<std::string>(iter, expr, dedup);
}

}   // namespace graph
}   // namespace nebula
