/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
#define NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_

#include "util/SchemaUtil.h"

namespace nebula {

class Expression;

namespace meta {
namespace cpp2 {

class ColumnDef;
class IndexItem;

}   // namespace cpp2
}   // namespace meta

namespace storage {
namespace cpp2 {

class IndexQueryContext;

}   // namespace cpp2
}   // namespace storage

namespace graph {

class OptimizerUtils {
public:
    enum class BoundValueOperator {
        GREATER_THAN = 0,
        LESS_THAN,
        MAX,
        MIN,
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

    static bool findOptimalIndex(
        int32_t schemaId,
        const Expression* condition,
        std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>* indexItems,
        bool* isPrefixScan,
        nebula::storage::cpp2::IndexQueryContext* ictx);
};

}   // namespace graph
}   // namespace nebula
#endif   // NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
