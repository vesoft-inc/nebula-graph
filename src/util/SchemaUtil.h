/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_SCHEMAUTIL_H_
#define UTIL_SCHEMAUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/expression/Expression.h"
#include "common/datatypes/DataSet.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class SchemaUtil final {
public:
    SchemaUtil() = delete;

public:
    static Status validateColumns(const std::vector<ColumnSpecification*> &columnSpecs,
                                  meta::cpp2::Schema &schema);

    static Status validateProps(const std::vector<SchemaPropItem*> &schemaProps,
                                meta::cpp2::Schema &schema);

    static StatusOr<nebula::Value> toSchemaValue(const meta::cpp2::PropertyType type,
                                                 const Value &v);

    // Conver int64 or string to Timestamp
    static StatusOr<nebula::Timestamp> toTimestamp(const Value &v);

    // Conver int64 or string to Date
    static StatusOr<nebula::Date> toDate(const Value &v);

    // Conver int64 or string to DateTime
    static StatusOr<nebula::DateTime> toDateTime(const Value &v);

    static Status setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static Status setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static StatusOr<VertexID> toVertexID(Expression *expr);

    static StatusOr<std::vector<Value>> toValueVec(std::vector<Expression*> exprs);

    static StatusOr<DataSet> toDescSchema(const meta::cpp2::Schema &schema);

    static std::string typeToString(const meta::cpp2::ColumnDef &col);
};

}  // namespace graph
}  // namespace nebula
#endif  // UTIL_SCHEMAUTIL_H_
