/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
#define NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_

#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "util/SchemaUtil.h"

namespace nebula {

class Expression;

namespace graph {

class OptimizerUtils {
public:
    enum class BoundValueOperator {
        GREATER_THAN = 0,
        LESS_THAN,
        MAX,
        MIN,
    };

    // {2, 1, 0} >
    // {2, 1} >
    // {2, 0, 1} >
    // {2, 0} >
    // {2} >
    // {1, 2} >
    // {1, 1} >
    // {1}
    enum class IndexPriority : uint8_t {
        kNotEqual = 0,
        kRange,
        kPrefix,
    };

    struct PriorityColumnHint {
        storage::cpp2::IndexColumnHint hint;
        const Expression* expr;
        IndexPriority priority;
    };

    struct IndexResult {
        const meta::cpp2::IndexItem* index;
        std::unique_ptr<Expression> unusedExpr;
        std::vector<PriorityColumnHint> hints;

        bool operator<(const IndexResult& rhs) const {
            if (hints.empty()) return true;
            auto sz = std::min(hints.size(), rhs.hints.size());
            for (size_t i = 0; i < sz; i++) {
                if (hints[i].priority < rhs.hints[i].priority) {
                    return true;
                }
            }
            if (hints.size() < rhs.hints.size()) {
                return true;
            }

            return false;
        }
    };

    OptimizerUtils() = delete;

    static Value boundValue(const meta::cpp2::ColumnDef& col,
                            BoundValueOperator op,
                            const Value& v = Value());

    static Value boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithMax(const meta::cpp2::ColumnDef& col);

    static Value boundValueWithMin(const meta::cpp2::ColumnDef& col);

    static Value normalizeValue(const meta::cpp2::ColumnDef& col, const Value& v);

    static Status boundValue(Expression::Kind kind,
                             const Value& val,
                             const meta::cpp2::ColumnDef& col,
                             Value& begin,
                             Value& end);

    static StatusOr<IndexResult> selectIndex(const Expression* expr,
                                             const meta::cpp2::IndexItem& index);
};

}   // namespace graph
}   // namespace nebula
#endif   // NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
