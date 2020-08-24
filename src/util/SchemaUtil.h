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
#include "common/meta/NebulaSchemaProvider.h"

#include "parser/MaintainSentences.h"
#include "util/GraphStatus.h"

namespace nebula {
namespace graph {

class SchemaUtil final {
public:
    SchemaUtil() = delete;

public:
    static GraphStatus validateColumns(const std::vector<ColumnSpecification*> &columnSpecs,
                                       meta::cpp2::Schema &schema);

    static GraphStatus validateProps(const std::vector<SchemaPropItem*> &schemaProps,
                                     meta::cpp2::Schema &schema);

    static std::shared_ptr<const meta::NebulaSchemaProvider>
    generateSchemaProvider(const SchemaVer ver, const meta::cpp2::Schema &schema);

    static StatusOr<nebula::Value> toSchemaValue(const meta::cpp2::PropertyType type,
                                                 const Value &v);

    static GraphStatus setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static GraphStatus setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static StatusOr<VertexID> toVertexID(Expression *expr);

    static StatusOr<std::vector<Value>> toValueVec(std::vector<Expression*> exprs);

    static DataSet toDescSchema(const meta::cpp2::Schema &schema);

    static DataSet toShowCreateSchema(bool isTag,
                                      const std::string &name,
                                      const meta::cpp2::Schema &schema);

    static std::string typeToString(const meta::cpp2::ColumnDef &col);

    static Value::Type propTypeToValueType(meta::cpp2::PropertyType propType);
};

}  // namespace graph
}  // namespace nebula
#endif  // UTIL_SCHEMAUTIL_H_
