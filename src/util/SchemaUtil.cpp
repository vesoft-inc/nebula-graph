/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"
#include "common/function/TimeFunction.h"
#include "util/SchemaUtil.h"
#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

// static
Status SchemaUtil::validateColumns(const std::vector<ColumnSpecification*> &columnSpecs,
                                   meta::cpp2::Schema &schema) {
    auto status = Status::OK();
    std::unordered_set<std::string> nameSet;
    for (auto& spec : columnSpecs) {
        if (nameSet.find(*spec->name()) != nameSet.end()) {
            return Status::Error("Duplicate column name `%s'", spec->name()->c_str());
        }
        nameSet.emplace(*spec->name());
        meta::cpp2::ColumnDef column;
        column.set_name(*spec->name());
        auto type = spec->type();
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(type);
        column.set_type(std::move(typeDef));
        column.set_nullable(spec->isNull());
        if (meta::cpp2::PropertyType::FIXED_STRING == type) {
            column.type.set_type_length(spec->typeLen());
        }

        if (spec->hasDefaultValue()) {
            auto value = spec->getDefaultValue();
            auto valStatus = toSchemaValue(type, value);
            if (!valStatus.ok()) {
                return valStatus.status();
            }
            column.set_default_value(std::move(valStatus).value());
        }
        schema.columns.emplace_back(std::move(column));
    }

    return Status::OK();
}

// static
Status SchemaUtil::validateProps(const std::vector<SchemaPropItem*> &schemaProps,
                                 meta::cpp2::Schema &schema) {
    auto status = Status::OK();
    if (!schemaProps.empty()) {
        for (auto& schemaProp : schemaProps) {
            switch (schemaProp->getPropType()) {
                case SchemaPropItem::TTL_DURATION:
                    status = setTTLDuration(schemaProp, schema);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
                case SchemaPropItem::TTL_COL:
                    status = setTTLCol(schemaProp, schema);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
            }
        }

        if (schema.schema_prop.get_ttl_duration() &&
            (*schema.schema_prop.get_ttl_duration() != 0)) {
            // Disable implicit TTL mode
            if (!schema.schema_prop.get_ttl_col() ||
                (schema.schema_prop.get_ttl_col() && schema.schema_prop.get_ttl_col()->empty())) {
                return Status::Error("Implicit ttl_col not support");
            }
        }
    }

    return Status::OK();
}

// static
std::shared_ptr<const meta::NebulaSchemaProvider>
SchemaUtil::generateSchemaProvider(const SchemaVer ver, const meta::cpp2::Schema &schema) {
    auto schemaPtr = std::make_shared<meta::NebulaSchemaProvider>(ver);
    for (auto col : schema.get_columns()) {
        bool hasDef = col.__isset.default_value;
        schemaPtr->addField(col.get_name(),
                            col.get_type().get_type(),
                            col.type.__isset.type_length ? *col.get_type().get_type_length() : 0,
                            col.__isset.nullable ? *col.get_nullable() : false,
                            hasDef ? *col.get_default_value() : Value());
    }
    return schemaPtr;
}

// static
StatusOr<nebula::Value> SchemaUtil::toSchemaValue(const meta::cpp2::PropertyType type,
                                                  const Value &v) {
    switch (type) {
        case meta::cpp2::PropertyType::TIMESTAMP: {
            if (v.type() != Value::Type::INT && v.type() != Value::Type::STRING) {
                LOG(ERROR) << "ValueType is wrong, input type "
                           << static_cast<int32_t>(type)
                           << ", v type " <<  v.type();
                return Status::Error("Wrong type");
            }
            auto timestamp = TimeFunction::toTimestamp(v);
            if (!timestamp.ok()) {
                return timestamp.status();
            }
            return Value(timestamp.value());
        }
        case meta::cpp2::PropertyType::DATE: {
            if (v.type() != Value::Type::INT && v.type() != Value::Type::STRING) {
                LOG(ERROR) << "ValueType is wrong, input type "
                           << static_cast<int32_t>(type)
                           << ", v type " <<  v.type();
                return Status::Error("Wrong type");
            }
            auto date = TimeFunction::toDate(v);
            if (!date.ok()) {
                return date.status();
            }
            return Value(date.value());
        }
        case meta::cpp2::PropertyType::DATETIME: {
            if (v.type() != Value::Type::INT && v.type() != Value::Type::STRING) {
                LOG(ERROR) << "ValueType is wrong, input type "
                           << static_cast<int32_t>(type)
                           << ", v type " <<  v.type();
                return Status::Error("Wrong type");
            }
            auto datetime = TimeFunction::toDateTime(v);
            if (!datetime.ok()) {
                return datetime.status();
            }
            return Value(datetime.value());
        }
        default: {
            return v;
        }
    }
}


// static
Status SchemaUtil::setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlDuration();
    if (!ret.ok()) {
        return ret.status();
    }

    auto ttlDuration = ret.value();
    schema.schema_prop.set_ttl_duration(ttlDuration);
    return Status::OK();
}


// static
Status SchemaUtil::setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema) {
    auto ret = schemaProp->getTtlCol();
    if (!ret.ok()) {
        return ret.status();
    }

    auto  ttlColName = ret.value();
    if (ttlColName.empty()) {
        schema.schema_prop.set_ttl_col("");
        return Status::OK();
    }
    // Check the legality of the ttl column name
    for (auto& col : schema.columns) {
        if (col.name == ttlColName) {
            // Only integer columns and timestamp columns can be used as ttl_col
            // TODO(YT) Ttl_duration supports datetime type
            if (col.type.type != meta::cpp2::PropertyType::INT64 &&
                col.type.type != meta::cpp2::PropertyType::TIMESTAMP) {
                return Status::Error("Ttl column type illegal");
            }
            schema.schema_prop.set_ttl_col(ttlColName);
            return Status::OK();
        }
    }
    return Status::Error("Ttl column name not exist in columns");
}

// static
StatusOr<VertexID> SchemaUtil::toVertexID(Expression *expr) {
    QueryExpressionContext ctx;
    auto vertexId = expr->eval(ctx(nullptr));
    if (vertexId.type() != Value::Type::STRING) {
        LOG(ERROR) << "Wrong vertex id type";
        return Status::Error("Wrong vertex id type");
    }
    return vertexId.getStr();
}

// static
StatusOr<std::vector<Value>>
SchemaUtil::toValueVec(std::vector<Expression*> exprs) {
    std::vector<Value> values;
    values.reserve(exprs.size());
    QueryExpressionContext ctx;
    for (auto *expr : exprs) {
        auto value = expr->eval(ctx(nullptr));
         if (value.isNull() && value.getNull() != NullType::__NULL__) {
            LOG(ERROR) << "Wrong value type: " << value.type();;
            return Status::Error("Wrong value type");
        }
        values.emplace_back(std::move(value));
    }
    return values;
}

StatusOr<DataSet> SchemaUtil::toDescSchema(const meta::cpp2::Schema &schema) {
    DataSet dataSet({"Field", "Type", "Null", "Default"});
    for (auto &col : schema.get_columns()) {
        Row row;
        row.values.emplace_back(Value(col.get_name()));
        row.values.emplace_back(typeToString(col));
        auto nullable = col.__isset.nullable ? *col.get_nullable() : false;
        row.values.emplace_back(nullable ? "YES" : "NO");
        auto defaultValue = col.__isset.default_value ? *col.get_default_value() : Value();
        row.values.emplace_back(std::move(defaultValue));
        dataSet.emplace_back(std::move(row));
    }
    return dataSet;
}

StatusOr<DataSet> SchemaUtil::toShowCreateSchema(bool isTag,
                                                 const std::string &name,
                                                 const meta::cpp2::Schema &schema) {
    DataSet dataSet;
    std::string createStr;
    createStr.resize(1024);
    if (isTag) {
        dataSet.colNames = {"Tag", "Create Tag"};
        createStr = "CREATE TAG `" + name + "` (\n";
    } else {
        dataSet.colNames = {"Edge", "Create Edge"};
        createStr = "CREATE EDGE `" + name + "` (\n";
    }
    Row row;
    row.emplace_back(name);
    for (auto &col : schema.get_columns()) {
        createStr += " `" + col.get_name() + "`";
        createStr += " " + typeToString(col);
        auto nullable = col.__isset.nullable ? *col.get_nullable() : false;
        if (!nullable) {
            createStr += " NOT NULL";
        } else {
            createStr += " NULL";
        }

        if (col.__isset.default_value) {
            auto value = col.get_default_value();
            auto toStr = value->toString();
            if (value->isNumeric() || value->isBool()) {
                createStr += " DEFAULT " + toStr;
            } else {
                createStr += " DEFAULT \"" + toStr + "\"";
            }
        }
        createStr += ",\n";
    }
    if (!schema.columns.empty()) {
        createStr.resize(createStr.size() -2);
        createStr += "\n";
    }
    createStr += ")";
    auto prop = schema.get_schema_prop();
    createStr += " ttl_duration = ";
    if (prop.__isset.ttl_duration) {
        createStr += folly::to<std::string>(*prop.get_ttl_duration());
    } else {
        createStr += "0";
    }
    createStr += ", ttl_col = ";
    if (prop.__isset.ttl_col && !(prop.get_ttl_col()->empty())) {
        createStr += "\"" + *prop.get_ttl_col() + "\"";
    } else {
        createStr += "\"\"";
    }
    row.emplace_back(std::move(createStr));
    dataSet.rows.emplace_back(std::move(row));
    return dataSet;
}

std::string SchemaUtil::typeToString(const meta::cpp2::ColumnTypeDef &col) {
    auto type = meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(col.get_type());
    if (col.get_type() == meta::cpp2::PropertyType::FIXED_STRING) {
        return folly::stringPrintf("%s(%d)", type, *col.get_type_length());
    }
    return type;
}

std::string SchemaUtil::typeToString(const meta::cpp2::ColumnDef &col) {
    auto str = typeToString(col.get_type());
    std::transform(
        std::begin(str), std::end(str), std::begin(str), [](uint8_t c) { return std::tolower(c); });
    return str;
}

Value::Type SchemaUtil::propTypeToValueType(meta::cpp2::PropertyType propType) {
    switch (propType) {
        case meta::cpp2::PropertyType::BOOL:
            return Value::Type::BOOL;
        case meta::cpp2::PropertyType::INT8:
        case meta::cpp2::PropertyType::INT16:
        case meta::cpp2::PropertyType::INT32:
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP:
            return Value::Type::INT;
        case meta::cpp2::PropertyType::TIME:
            return Value::Type::TIME;
        case meta::cpp2::PropertyType::VID:
            return Value::Type::STRING;
        case meta::cpp2::PropertyType::FLOAT:
        case meta::cpp2::PropertyType::DOUBLE:
            return Value::Type::FLOAT;
        case meta::cpp2::PropertyType::STRING:
        case meta::cpp2::PropertyType::FIXED_STRING:
            return Value::Type::STRING;
        case meta::cpp2::PropertyType::DATE:
            return Value::Type::DATE;
        case meta::cpp2::PropertyType::DATETIME:
            return Value::Type::DATETIME;
        case meta::cpp2::PropertyType::UNKNOWN:
            return Value::Type::__EMPTY__;
    }
    return Value::Type::__EMPTY__;
}

bool SchemaUtil::isValidVid(const Value &value, const meta::cpp2::ColumnTypeDef &type) {
    return isValidVid(value, type.get_type());
}

bool SchemaUtil::isValidVid(const Value &value, meta::cpp2::PropertyType type) {
    auto vidType = propTypeToValueType(type);
    if (!isValidVid(value) || value.type() != vidType) {
        return false;
    }
    return true;
}

bool SchemaUtil::isValidVid(const Value &value) {
    if (!value.isStr() && !value.isInt()) {  // compatible with 1.0
        return false;
    }
    return true;
}
}  // namespace graph
}  // namespace nebula
