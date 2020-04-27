/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "SchemaUtil.h"

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
        auto type = spec->type();
        column.set_type(type);
        column.set_nullable(spec->isNull());
        if (meta::cpp2::PropertyType::FIXED_STRING == type) {
            column.set_type_length(spec->typeLen());
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
            auto timestamp = toTimestamp(v);
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
            auto date = toDate(v);
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
            auto datetime = toDateTime(v);
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
StatusOr<nebula::Timestamp> SchemaUtil::toTimestamp(const Value &) {
    nebula::Timestamp timestamp = 0;
    return timestamp;
}

// static
StatusOr<nebula::Date> SchemaUtil::toDate(const Value &) {
    nebula::Date date;
    date.year = 0;
    date.month = 0;
    date.day = 0;
    return date;
}

// static
StatusOr<nebula::DateTime> SchemaUtil::toDateTime(const Value &) {
    nebula::DateTime dateTime;
    dateTime.year = 0;
    dateTime.month = 0;
    dateTime.day = 0;
    dateTime.hour = 0;
    dateTime.minute = 0;
    dateTime.sec = 0;
    dateTime.microsec = 0;
    dateTime.timezone = 0;
    return dateTime;
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
    // Check the legality of the ttl column name
    for (auto& col : schema.columns) {
        if (col.name == ttlColName) {
            if (col.type != meta::cpp2::PropertyType::INT64 &&
                col.type != meta::cpp2::PropertyType::TIMESTAMP &&
                col.type != meta::cpp2::PropertyType::DATE &&
                col.type != meta::cpp2::PropertyType::DATETIME) {
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
    auto vertexId = expr->eval();
    if (vertexId.isNull() ||
            (vertexId.type() != Value::Type::INT && vertexId.type() != Value::Type::STRING)) {
        LOG(ERROR) << "Wrong vertex id type";
        return Status::Error("Wrong vertex id type");
    }
    return expr->toString();
}

// static
StatusOr<std::vector<Value>>
SchemaUtil::toValueVec(std::vector<Expression*> exprs) {
    std::vector<Value> values;
    values.reserve(exprs.size());
    for (auto *expr : exprs) {
        auto value = expr->eval();
         if (value.isNull()) {
            LOG(ERROR) << "Wrong value type";
            return Status::Error("Wrong value type");
        }
        values.emplace_back(std::move(value));
    }
    return values;
}

StatusOr<DataSet> SchemaUtil::toDescSchema(const meta::cpp2::Schema &schema) {
    DataSet dataSet;
    dataSet.colNames = {"Field", "Type", "Null", "Default"};
    for (auto &col : schema.get_columns()) {
        std::vector<Value> row;
        row.emplace_back(Value(col.get_name()));
        row.emplace_back(typeToString(col));
        auto nullable = col.__isset.nullable ? *col.get_nullable() : false;
        row.emplace_back(nullable ? "YES" : "NO");
        row.emplace_back(col.get_default_value());
    }
    return dataSet;
}

std::string SchemaUtil::typeToString(const meta::cpp2::ColumnDef &col) {
    switch (col.get_type()) {
        case meta::cpp2::PropertyType::BOOL:
            return "bool";
        case meta::cpp2::PropertyType::INT8:
            return "int8";
        case meta::cpp2::PropertyType::INT16:
            return "int16";
        case meta::cpp2::PropertyType::INT32:
            return "int32";
        case meta::cpp2::PropertyType::INT64:
            return "int64";
        case meta::cpp2::PropertyType::VID:
            return "vid";
        case meta::cpp2::PropertyType::FLOAT:
            return "float";
        case meta::cpp2::PropertyType::DOUBLE:
            return "double";
        case meta::cpp2::PropertyType::STRING:
            return "string";
        case meta::cpp2::PropertyType::FIXED_STRING: {
            auto typeLen = col.__isset.type_length ? *col.get_type_length() : 0;
            return folly::stringPrintf("fixed_string(%d)", typeLen);
        }
        case meta::cpp2::PropertyType::TIMESTAMP:
            return "timestamp";
        case meta::cpp2::PropertyType::DATE:
            return "date";
        case meta::cpp2::PropertyType::DATETIME:
            return "datetime";
        case meta::cpp2::PropertyType::UNKNOWN:
            return "";
    }
    return "";
}
}  // namespace graph
}  // namespace nebula
