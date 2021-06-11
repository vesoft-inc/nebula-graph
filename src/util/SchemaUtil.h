/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_SCHEMAUTIL_H_
#define UTIL_SCHEMAUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"
#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "parser/MaintainSentences.h"

using VertexPropsPtr = std::unique_ptr<std::vector<nebula::storage::cpp2::VertexProp>>;
using EdgePropsPtr = std::unique_ptr<std::vector<nebula::storage::cpp2::EdgeProp>>;
namespace nebula {
namespace graph {
class QueryContext;
struct SpaceInfo;
class SchemaUtil final {
public:
    SchemaUtil() = delete;

public:
    static Status validateProps(const std::vector<SchemaPropItem*> &schemaProps,
                                meta::cpp2::Schema &schema);

    static std::shared_ptr<const meta::NebulaSchemaProvider>
    generateSchemaProvider(const SchemaVer ver, const meta::cpp2::Schema &schema);

    static Status setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static Status setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static Status setComment(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

    static StatusOr<Value> toVertexID(Expression *expr, Value::Type vidType);

    static StatusOr<std::vector<Value>> toValueVec(std::vector<Expression*> exprs);

    static StatusOr<DataSet> toDescSchema(const meta::cpp2::Schema &schema);

    static StatusOr<DataSet> toShowCreateSchema(bool isTag,
                                                const std::string &name,
                                                const meta::cpp2::Schema &schema);

    static std::string typeToString(const meta::cpp2::ColumnTypeDef &col);
    static std::string typeToString(const meta::cpp2::ColumnDef &col);

    static Value::Type propTypeToValueType(meta::cpp2::PropertyType propType);

    static bool isValidVid(const Value &value, const meta::cpp2::ColumnTypeDef &type);

    static bool isValidVid(const Value& value, meta::cpp2::PropertyType type);

    static bool isValidVid(const Value& value);

    // Fetch all tags in the space and retrieve props from tags
    // only take _tag when withProp is true
    static StatusOr<VertexPropsPtr> getAllVertexProp(QueryContext* qctx,
                                                     const SpaceInfo& space,
                                                     bool withProp);

    // retrieve prop from specific edgetypes
    // only take _src _dst _type _rank when withProp is true
    static StatusOr<EdgePropsPtr> getEdgeProps(QueryContext* qctx,
                                               const SpaceInfo& space,
                                               const std::vector<EdgeType>& edgeTypes,
                                               bool withProp);
};

}  // namespace graph
}  // namespace nebula
#endif  // UTIL_SCHEMAUTIL_H_
