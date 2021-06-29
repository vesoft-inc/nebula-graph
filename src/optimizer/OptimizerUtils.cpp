/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptimizerUtils.h"
#include <algorithm>
#include <memory>
#include "common/base/Status.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"

using nebula::meta::cpp2::ColumnDef;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexColumnHint;

using IndexResult = nebula::graph::OptimizerUtils::IndexResult;
using IndexPriority = nebula::graph::OptimizerUtils::IndexPriority;
using PriorityColumnHint = nebula::graph::OptimizerUtils::PriorityColumnHint;
using BVO = nebula::graph::OptimizerUtils::BoundValueOperator;

namespace nebula {
namespace graph {

Value OptimizerUtils::boundValue(const meta::cpp2::ColumnDef& col,
                                 BoundValueOperator op,
                                 const Value& v) {
    switch (op) {
        case BoundValueOperator::GREATER_THAN : {
            return boundValueWithGT(col, v);
        }
        case BoundValueOperator::LESS_THAN : {
            return boundValueWithLT(col, v);
        }
        case BoundValueOperator::MAX : {
            return boundValueWithMax(col);
        }
        case BoundValueOperator::MIN : {
            return boundValueWithMin(col);
        }
    }
    return Value::kNullBadType;
}

Value OptimizerUtils::boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type().get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::max()) {
                return v;
            } else {
                return v + 1;
            }
        }
        case Value::Type::FLOAT : {
            if (v.getFloat() > 0.0) {
                if (v.getFloat() == std::numeric_limits<double_t>::max()) {
                    return v;
                }
            } else if (v.getFloat() == 0.0) {
                return Value(std::numeric_limits<double_t>::min());
            } else {
                if (v.getFloat() == -std::numeric_limits<double_t>::min()) {
                    return Value(0.0);
                }
            }
            return v.getFloat() + kEpsilon;
        }
        case Value::Type::STRING : {
            if (!col.type.type_length_ref().has_value()) {
                return Value::kNullBadType;
            }
            std::vector<unsigned char> bytes(v.getStr().begin(), v.getStr().end());
            bytes.resize(*col.get_type().type_length_ref());
            for (size_t i = bytes.size();; i--) {
                if (i > 0) {
                    if (bytes[i-1]++ != 255) break;
                } else {
                    return Value(std::string(*col.get_type().type_length_ref(), '\377'));
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            if (Date(std::numeric_limits<int16_t>::max(), 12, 31) == v.getDate()) {
                return v.getDate();
            } else if (Date() == v.getDate()) {
                return Date(0, 1, 2);
            }
            auto d = v.getDate();
            if (d.day < 31) {
                d.day += 1;
            } else {
                d.day = 1;
                if (d.month < 12) {
                    d.month += 1;
                } else {
                    d.month = 1;
                    if (d.year < std::numeric_limits<int16_t>::max()) {
                        d.year += 1;
                    } else {
                        return v.getDate();
                    }
                }
            }
            return Value(d);
        }
        case Value::Type::TIME : {
            auto t = v.getTime();
            // Ignore the time zone.
            if (t.microsec < 999999) {
                t.microsec = t.microsec + 1;
            } else {
                t.microsec = 0;
                if (t.sec < 59) {
                    t.sec += 1;
                } else {
                    t.sec = 0;
                    if (t.minute < 59) {
                        t.minute += 1;
                    } else {
                        t.minute = 0;
                        if (t.hour < 23) {
                            t.hour += 1;
                        } else {
                            return v.getTime();
                        }
                    }
                }
            }
            return Value(t);
        }
        case Value::Type::DATETIME : {
            auto dt = v.getDateTime();
            // Ignore the time zone.
            if (dt.microsec < 999999) {
                dt.microsec = dt.microsec + 1;
            } else {
                dt.microsec = 0;
                if (dt.sec < 59) {
                    dt.sec += 1;
                } else {
                    dt.sec = 0;
                    if (dt.minute < 59) {
                        dt.minute += 1;
                    } else {
                        dt.minute = 0;
                        if (dt.hour < 23) {
                            dt.hour += 1;
                        } else {
                            dt.hour = 0;
                            if (dt.day < 31) {
                                dt.day += 1;
                            } else {
                                dt.day = 1;
                                if (dt.month < 12) {
                                    dt.month += 1;
                                } else {
                                    dt.month = 1;
                                    if (dt.year < std::numeric_limits<int16_t>::max()) {
                                        dt.year += 1;
                                    } else {
                                        return v.getDateTime();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Value(dt);
        }
        case Value::Type::__EMPTY__:
        case Value::Type::BOOL:
        case Value::Type::NULLVALUE:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::LIST:
        case Value::Type::SET:
        case Value::Type::MAP:
        case Value::Type::DATASET:
        case Value::Type::PATH: {
            DLOG(FATAL) << "Not supported value type " << type
                        << "for index.";
            return Value::kNullBadType;
        }
    }
    DLOG(FATAL) << "Unknown value type " << static_cast<int>(type);
    return Value::kNullBadType;
}

Value OptimizerUtils::boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type().get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::min()) {
                return v;
            } else {
                return v - 1;
            }
        }
        case Value::Type::FLOAT : {
            if (v.getFloat() < 0.0) {
                if (v.getFloat() == -std::numeric_limits<double_t>::max()) {
                    return v;
                } else if (v.getFloat() == -std::numeric_limits<double_t>::min()) {
                    return Value(0.0);
                }
            } else if (v.getFloat() == 0.0) {
                return Value(-std::numeric_limits<double_t>::min());
            }
            return v.getFloat() - kEpsilon;
        }
        case Value::Type::STRING : {
            if (!col.type.type_length_ref().has_value()) {
                return Value::kNullBadType;
            }
            std::vector<unsigned char> bytes(v.getStr().begin(), v.getStr().end());
            bytes.resize(*col.get_type().type_length_ref());
            for (size_t i = bytes.size();; i--) {
                if (i > 0) {
                    if (bytes[i-1]-- != 0) break;
                } else {
                    return Value(std::string(*col.get_type().type_length_ref(), '\0'));
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            if (Date() == v.getDate()) {
                return v.getDate();
            }
            auto d = v.getDate();
            if (d.day > 1) {
                d.day -= 1;
            } else {
                d.day = 31;
                if (d.month > 1) {
                    d.month -= 1;
                } else {
                    d.month = 12;
                    if (d.year > 1) {
                        d.year -= 1;
                    } else {
                        return v.getDate();
                    }
                }
            }
            return Value(d);
        }
        case Value::Type::TIME : {
            if (Time() == v.getTime()) {
                return v.getTime();
            }
            auto t = v.getTime();
            if (t.microsec >= 1) {
                t.microsec -= 1;
            } else {
                t.microsec = 999999;
                if (t.sec >= 1) {
                    t.sec -= 1;
                } else {
                    t.sec = 59;
                    if (t.minute >= 1) {
                        t.minute -= 1;
                    } else {
                        t.minute = 59;
                        if (t.hour >= 1) {
                            t.hour -= 1;
                        } else {
                            return v.getTime();
                        }
                    }
                }
            }
            return Value(t);
        }
        case Value::Type::DATETIME : {
            if (DateTime() == v.getDateTime()) {
                return v.getDateTime();
            }
            auto dt = v.getDateTime();
            if (dt.microsec >= 1) {
                dt.microsec -= 1;
            } else {
                dt.microsec = 999999;
                if (dt.sec >= 1) {
                    dt.sec -= 1;
                } else {
                    dt.sec = 59;
                    if (dt.minute >= 1) {
                        dt.minute -= 1;
                    } else {
                        dt.minute = 59;
                        if (dt.hour >= 1) {
                            dt.hour -= 1;
                        } else {
                            dt.hour = 23;
                            if (dt.day > 1) {
                                dt.day -= 1;
                            } else {
                                dt.day = 31;
                                if (dt.month > 1) {
                                    dt.month -= 1;
                                } else {
                                    dt.month = 12;
                                    if (dt.year > 1) {
                                        dt.year -= 1;
                                    } else {
                                        return v.getDateTime();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Value(dt);
        }
        case Value::Type::__EMPTY__:
        case Value::Type::BOOL:
        case Value::Type::NULLVALUE:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::LIST:
        case Value::Type::SET:
        case Value::Type::MAP:
        case Value::Type::DATASET:
        case Value::Type::PATH: {
            DLOG(FATAL) << "Not supported value type " << type
                        << "for index.";
            return Value::kNullBadType;
        }
    }
    DLOG(FATAL) << "Unknown value type " << static_cast<int>(type);
    return Value::kNullBadType;
}

Value OptimizerUtils::boundValueWithMax(const meta::cpp2::ColumnDef& col) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type().get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::max());
        }
        case Value::Type::FLOAT : {
            return Value(std::numeric_limits<double>::max());
        }
        case Value::Type::STRING : {
            if (!col.type.type_length_ref().has_value()) {
                return Value::kNullBadType;
            }
            return Value(std::string(*col.get_type().type_length_ref(), '\377'));
        }
        case Value::Type::DATE : {
            Date d;
            d.year = std::numeric_limits<int16_t>::max();
            d.month = 12;
            d.day = 31;
            return Value(d);
        }
        case Value::Type::TIME: {
            Time dt;
            dt.hour = 23;
            dt.minute = 59;
            dt.sec = 59;
            dt.microsec = 999999;
            return Value(dt);
        }
        case Value::Type::DATETIME: {
            DateTime dt;
            dt.year = std::numeric_limits<int16_t>::max();
            dt.month = 12;
            dt.day = 31;
            dt.hour = 23;
            dt.minute = 59;
            dt.sec = 59;
            dt.microsec = 999999;
            return Value(dt);
        }
        case Value::Type::__EMPTY__:
        case Value::Type::BOOL:
        case Value::Type::NULLVALUE:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::LIST:
        case Value::Type::SET:
        case Value::Type::MAP:
        case Value::Type::DATASET:
        case Value::Type::PATH: {
            DLOG(FATAL) << "Not supported value type " << type
                        << "for index.";
            return Value::kNullBadType;
        }
    }
    DLOG(FATAL) << "Unknown value type " << static_cast<int>(type);
    return Value::kNullBadType;
}

Value OptimizerUtils::boundValueWithMin(const meta::cpp2::ColumnDef& col) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type().get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::min());
        }
        case Value::Type::FLOAT : {
            return Value(-std::numeric_limits<double>::max());
        }
        case Value::Type::STRING : {
            if (!col.type.type_length_ref().has_value()) {
                return Value::kNullBadType;
            }
            return Value(std::string(*col.get_type().type_length_ref(), '\0'));
        }
        case Value::Type::DATE : {
            return Value(Date());
        }
        case Value::Type::TIME: {
            return Value(Time());
        }
        case Value::Type::DATETIME : {
            return Value(DateTime());
        }
        case Value::Type::__EMPTY__:
        case Value::Type::BOOL:
        case Value::Type::NULLVALUE:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::LIST:
        case Value::Type::SET:
        case Value::Type::MAP:
        case Value::Type::DATASET:
        case Value::Type::PATH: {
            DLOG(FATAL) << "Not supported value type " << type
                        << "for index.";
            return Value::kNullBadType;
        }
    }
    DLOG(FATAL) << "Unknown value type " << static_cast<int>(type);
    return Value::kNullBadType;
}

Value OptimizerUtils::normalizeValue(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type().get_type());
    switch (type) {
        case Value::Type::INT:
        case Value::Type::FLOAT:
        case Value::Type::BOOL:
        case Value::Type::DATE:
        case Value::Type::TIME:
        case Value::Type::DATETIME: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.type.type_length_ref().has_value()) {
                return Value::kNullBadType;
            }
            if (!v.isStr()) {
                return v;
            }
            auto len = static_cast<size_t>(*col.get_type().get_type_length());
            if (v.getStr().size() > len) {
                return Value(v.getStr().substr(0, len));
            } else {
                std::string s;
                s.reserve(len);
                s.append(v.getStr()).append(len - v.getStr().size(), '\0');
                return Value(std::move(s));
            }
        }
        case Value::Type::__EMPTY__:
        case Value::Type::NULLVALUE:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::LIST:
        case Value::Type::SET:
        case Value::Type::MAP:
        case Value::Type::DATASET:
        case Value::Type::PATH: {
            DLOG(FATAL) << "Not supported value type " << type
                        << "for index.";
            return Value::kNullBadType;
        }
    }
    DLOG(FATAL) << "Unknown value type " << static_cast<int>(type);
    return Value::kNullBadType;;
}

Status OptimizerUtils::boundValue(Expression::Kind kind,
                                  const Value& val,
                                  const meta::cpp2::ColumnDef& col,
                                  Value& begin,
                                  Value& end) {
    if (val.type() != graph::SchemaUtil::propTypeToValueType(col.type.type)) {
        return Status::SemanticError("Data type error of field : %s", col.get_name().c_str());
    }
    switch (kind) {
        case Expression::Kind::kRelLE: {
            // if c1 <= int(5) , the range pair should be (min, 6)
            // if c1 < int(5), the range pair should be (min, 5)
            auto v = OptimizerUtils::boundValue(col, BoundValueOperator::GREATER_THAN, val);
            if (v == Value::kNullBadType) {
                LOG(ERROR) << "Get bound value error. field : " << col.get_name();
                return Status::Error("Get bound value error. field : %s", col.get_name().c_str());
            }
            // where c <= 1 and c <= 2 , 1 should be valid.
            if (end == Value()) {
                end = v;
            } else {
                end = v < end ? v : end;
            }
            break;
        }
        case Expression::Kind::kRelGE: {
            // where c >= 1 and c >= 2 , 2 should be valid.
            if (begin == Value()) {
                begin = val;
            } else {
                begin = val < begin ? begin : val;
            }
            break;
        }
        case Expression::Kind::kRelLT: {
            // c < 5 and c < 6 , 5 should be valid.
            if (end == Value()) {
                end = val;
            } else {
                end = val < end ? val : end;
            }
            break;
        }
        case Expression::Kind::kRelGT: {
            // if c >= 5, the range pair should be (5, max)
            // if c > 5, the range pair should be (6, max)
            auto v = OptimizerUtils::boundValue(col, BoundValueOperator::GREATER_THAN, val);
            if (v == Value::kNullBadType) {
                LOG(ERROR) << "Get bound value error. field : " << col.get_name();
                return Status::Error("Get bound value error. field : %s", col.get_name().c_str());
            }
            // where c > 1 and c > 2 , 2 should be valid.
            if (begin == Value()) {
                begin = v;
            } else {
                begin = v < begin ? begin : v;
            }
            break;
        }
        default: {
            // TODO(yee): Semantic error
            return Status::Error("Invalid expression kind.");
        }
    }
    return Status::OK();
}

Status checkValue(const ColumnDef& field, BVO bvo, Value* value) {
    if (*value == Value()) {
        *value = OptimizerUtils::boundValue(field, bvo, Value());
        if (*value == Value::kNullBadType) {
            return Status::Error("Get bound value error. field : %s", field.get_name().c_str());
        }
    }
    return Status::OK();
}

Status handleRangeIndex(const meta::cpp2::ColumnDef& field,
                        const Expression* expr,
                        const Value& value,
                        IndexColumnHint* hint) {
    if (field.get_type().get_type() == meta::cpp2::PropertyType::BOOL) {
        return Status::Error("Range scan for bool type is illegal");
    }
    Value begin, end;
    NG_RETURN_IF_ERROR(OptimizerUtils::boundValue(expr->kind(), value, field, begin, end));
    NG_RETURN_IF_ERROR(checkValue(field, BVO::MIN, &begin));
    NG_RETURN_IF_ERROR(checkValue(field, BVO::MAX, &end));
    hint->set_begin_value(std::move(begin));
    hint->set_end_value(std::move(end));
    hint->set_scan_type(storage::cpp2::ScanType::RANGE);
    hint->set_column_name(field.get_name());
    return Status::OK();
}

void handleEqualIndex(const ColumnDef& field, const Value& value, IndexColumnHint* hint) {
    hint->set_scan_type(storage::cpp2::ScanType::PREFIX);
    hint->set_column_name(field.get_name());
    hint->set_begin_value(OptimizerUtils::normalizeValue(field, value));
}

StatusOr<PriorityColumnHint> selectRelExprIndex(const ColumnDef& field,
                                                const RelationalExpression* expr) {
    // TODO(yee): Reverse expression
    auto left = expr->left();
    DCHECK(left->kind() == Expression::Kind::kEdgeProperty ||
           left->kind() == Expression::Kind::kTagProperty);
    auto propExpr = static_cast<const PropertyExpression*>(left);
    if (propExpr->prop() != field.get_name()) {
        return Status::Error("Invalid field name.");
    }

    auto right = expr->right();
    DCHECK(right->kind() == Expression::Kind::kConstant);
    const auto& value = static_cast<const ConstantExpression*>(right)->value();

    PriorityColumnHint hint;
    switch (expr->kind()) {
        case Expression::Kind::kRelEQ: {
            handleEqualIndex(field, value, &hint.hint);
            hint.priority = IndexPriority::kPrefix;
            break;
        }
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelLT: {
            NG_RETURN_IF_ERROR(handleRangeIndex(field, expr, value, &hint.hint));
            hint.priority = IndexPriority::kRange;
            break;
        }
        case Expression::Kind::kRelNE: {
            hint.priority = IndexPriority::kNotEqual;
            break;
        }
        default: {
            return Status::Error("Invalid expression kind");
        }
    }
    hint.expr = expr;
    return hint;
}

StatusOr<IndexResult> selectRelExprIndex(const RelationalExpression* expr, const IndexItem& index) {
    const auto& fields = index.get_fields();
    if (fields.empty()) {
        return Status::Error("Index(%s) does not have any fields.", index.get_index_name().c_str());
    }
    auto status = selectRelExprIndex(fields[0], expr);
    NG_RETURN_IF_ERROR(status);
    IndexResult result;
    result.hints.emplace_back(std::move(status).value());
    result.index = &index;
    return result;
}

bool getIndexColumnHintInExpr(const ColumnDef& field,
                              const LogicalExpression* expr,
                              PriorityColumnHint* hint,
                              Expression** which) {
    for (auto& operand : expr->operands()) {
        if (!operand->isRelExpr()) continue;
        auto relExpr = static_cast<const RelationalExpression*>(operand.get());
        auto status = selectRelExprIndex(field, relExpr);
        if (status.ok()) {
            *hint = std::move(status).value();
            *which = operand.get();
            return true;
        }
    }
    return false;
}

std::unique_ptr<Expression> cloneUnusedExpr(const LogicalExpression* expr,
                                            const std::vector<Expression*>& usedOperands) {
    std::vector<std::unique_ptr<Expression>> unusedOperands;
    for (auto& operand : expr->operands()) {
        auto iter = std::find(usedOperands.begin(), usedOperands.end(), operand.get());
        if (iter == usedOperands.end()) {
            unusedOperands.emplace_back(operand->clone());
        }
    }
    if (unusedOperands.empty()) {
        return nullptr;
    }
    auto logExpr = std::make_unique<LogicalExpression>(expr->kind());
    logExpr->setOperands(std::move(unusedOperands));
    return logExpr;
}

StatusOr<IndexResult> selectLogicalExprIndex(const LogicalExpression* expr,
                                             const IndexItem& index) {
    if (expr->kind() != Expression::Kind::kLogicalAnd) {
        return Status::Error("Invalid expression kind.");
    }
    IndexResult result;
    result.hints.reserve(index.get_fields().size());
    std::vector<Expression*> usedOperands;
    for (auto& field : index.get_fields()) {
        PriorityColumnHint hint;
        Expression* operand = nullptr;
        if (!getIndexColumnHintInExpr(field, expr, &hint, &operand)) {
            break;
        }
        result.hints.emplace_back(std::move(hint));
        usedOperands.emplace_back(operand);
    }
    if (result.hints.empty()) {
        return Status::Error("There is not index to use.");
    }
    result.unusedExpr = cloneUnusedExpr(expr, usedOperands);
    result.index = &index;
    return result;
}

StatusOr<IndexResult> OptimizerUtils::selectIndex(const Expression* expr, const IndexItem& index) {
    if (expr->isRelExpr()) {
        return selectRelExprIndex(static_cast<const RelationalExpression*>(expr), index);
    }

    if (expr->isLogicalExpr()) {
        return selectLogicalExprIndex(static_cast<const LogicalExpression*>(expr), index);
    }

    return Status::Error("Invalid expression kind.");
}

}  // namespace graph
}  // namespace nebula
