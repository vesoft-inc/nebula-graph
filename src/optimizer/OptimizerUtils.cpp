/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptimizerUtils.h"
namespace nebula {
namespace graph {
using PropertyType = nebula::meta::cpp2::PropertyType;

static Value::Type toValueType(PropertyType type) {
    switch (type) {
        case PropertyType::BOOL :
            return Value::Type::BOOL;
        case PropertyType::INT64 :
        case PropertyType::INT32 :
        case PropertyType::INT16 :
        case PropertyType::INT8 :
        case PropertyType::TIMESTAMP :
            return Value::Type::INT;
        case PropertyType::VID :
            return Value::Type::VERTEX;
        case PropertyType::FLOAT :
        case PropertyType::DOUBLE :
            return Value::Type::FLOAT;
        case PropertyType::STRING :
        case PropertyType::FIXED_STRING :
            return Value::Type::STRING;
        case PropertyType::DATE :
            return Value::Type::DATE;
        case PropertyType::DATETIME :
            return Value::Type::DATETIME;
        case PropertyType::UNKNOWN :
            return Value::Type::__EMPTY__;
    }
}

Value OptimizerUtils::boundValue(const meta::cpp2::ColumnDef& col,
                                 BoundValueType op,
                                 const Value& v) {
    switch (op) {
        case BoundValueType::GREATER_THAN : {
            return boundValueWithGT(col, v);
        }
        case BoundValueType::LESS_THAN : {
            return boundValueWithLT(col, v);
        }
        case BoundValueType::MAX : {
            return boundValueWithMax(col, v);
        }
        case BoundValueType::MIN : {
            return Value(NullType::__NULL__);
        }
    }
}

Value OptimizerUtils::doubleWithGT(DoubleFormat df) {
    if (df.integral == 9 && df.decimal == 999999 && df.exponent == 308 && df.isPositiveExpon()) {
        return df.isPositive() ? Value(df.toDouble()) : Value(double(0));
    }
    // double default precision is 6, so the max value is 999999 of decimal part.
    if (df.decimal < 999999) {
        df.decimal = df.decimal + 1;
    } else if (df.integral < 9) {
        df.integral = df.integral + 1;
    } else if (df.isPositiveExpon()) {
        df.exponent = df.exponent + 1;
        df.decimal = 0;
        df.integral = 1;
    } else {
        if (df.exponent > 0) {
            df.exponent = df.exponent - 1;
        } else {
            df.exponSign = '+';
            df.exponent = 1;
        }
        df.decimal = 0;
        df.integral = 1;
    }
    return Value(df.toDouble());
}

Value OptimizerUtils::doubleWithLT(DoubleFormat df) {
    if (df.integral == 1 && df.decimal == 0 && df.exponent == 308 && !df.isPositiveExpon()) {
        return df.isPositive() ? Value(double(0)) : Value(df.toDouble());
    }
    if (df.decimal > 0) {
        df.decimal = df.decimal - 1;
    } else if (df.integral > 1) {
        df.integral = df.integral - 1;
    } else if (!df.isPositiveExpon()) {
        df.exponent = df.exponent + 1;
        df.decimal = 999999;
        df.integral = 9;
    } else {
        if (df.exponent > 0) {
            df.exponent = df.exponent - 1;
        } else {
            df.exponSign = '-';
            df.exponent = 1;
        }
        df.decimal = 999999;
        df.integral = 9;

    }
    return Value(df.toDouble());
}

Value OptimizerUtils::boundDoubleWithGT(DoubleFormat df) {
    if (df.integral == 0 and df.decimal == 0) {
        return Value(double(1.0E-308));
    }
    if (df.isPositive()) {
        return doubleWithGT(df);
    } else {
        return doubleWithLT(df);
    }
}

Value OptimizerUtils::boundDoubleWithLT(DoubleFormat df) {
    if (df.integral == 0 and df.decimal == 0) {
        return Value(double(-std::numeric_limits<double>::max()));
    }
    if (df.isPositive()) {
        return doubleWithLT(df);
    } else {
        return doubleWithGT(df);
    }
}

Value OptimizerUtils::boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = toValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::max()) {
                return v;
            } else {
                return v + 1;
            }
        }
        case Value::Type::FLOAT : {
            DoubleFormat df(v.getFloat());
            return boundDoubleWithGT(df);
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return v;
            }
            std::string str;
            str.reserve(*col.get_type_length());
            str.append(v.getStr());
            str.append(*col.get_type_length() - v.getStr().size(), '\0');
            std::vector<unsigned char> bytes(str.begin(), str.end());
            for (size_t i = bytes.size() - 1; i >= 0; i--) {
                if (bytes[i] < 255) {
                    bytes[i] += 1;
                    break;
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            auto d = v.getDate();
            if (d.day < 31) {
                d.day = d.day + 1;
            } else if (d.month < 12) {
                d.month = d.month + 1;
            } else if (d.year < std::numeric_limits<int16_t>::max()) {
                d.year = d.year + 1;
            }
            return Value(d);
        }
        case Value::Type::DATETIME : {
            auto dt = v.getDateTime();
            // Ignore the time zone.
            if (dt.microsec < std::numeric_limits<int32_t>::max()) {
                dt.microsec = dt.microsec + 1;
            } else if (dt.sec < 60) {
                dt.sec = dt.sec + 1;
            } else if (dt.minute < 60) {
                dt.minute = dt.minute + 1;
            } else if (dt.hour < 24) {
                dt.hour = dt.hour + 1;
            } else if (dt.day < 31) {
                dt.day = dt.day + 1;
            } else if (dt.month < 12) {
                dt.month = dt.month + 1;
            } else if (dt.year < std::numeric_limits<int16_t>::max()) {
                dt.year = dt.year + 1;
            }
            return Value(dt);
        }
        default : {
            return v;
        }
    }
}

Value OptimizerUtils::boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = toValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::min()) {
                return v;
            } else {
                return v - 1;
            }
        }
        case Value::Type::FLOAT : {
            DoubleFormat df(v.getFloat());
            return boundDoubleWithLT(df);
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return v;
            }
            std::string str;
            str.reserve(*col.get_type_length());
            str.append(v.getStr());
            str.append(*col.get_type_length() - v.getStr().size(), '\0');
            std::vector<unsigned char> bytes(str.begin(), str.end());
            for (size_t i = bytes.size() - 1; i >= 0; i--) {
                if (bytes[i] > 0) {
                    bytes[i] -= 1;
                    break;
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            auto d = v.getDate();
            if (d.day > 0) {
                d.day = d.day - 1;
            } else if (d.month > 0) {
                d.month = d.month - 1;
            } else if (d.year > 0) {
                d.year = d.year - 1;
            }
            return Value(d);
        }
        case Value::Type::DATETIME : {
            auto dt = v.getDateTime();
            // Ignore the time zone.
            if (dt.microsec > 0) {
                dt.sec = dt.sec - 1;
            } else if (dt.sec > 0) {
                dt.sec = dt.sec - 1;
            } else if (dt.minute > 0) {
                dt.minute = dt.minute - 1;
            } else if (dt.hour > 0) {
                dt.hour = dt.hour - 1;
            } else if (dt.day > 0) {
                dt.day = dt.day - 1;
            } else if (dt.month > 0) {
                dt.month = dt.month - 1;
            } else if (dt.year > 0){
                dt.year = dt.year - 1;
            }
            return Value(dt);
        }
        default :
            return v;
    }
}

Value OptimizerUtils::boundValueWithMax(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = toValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::max());
        }
        case Value::Type::FLOAT : {
            return Value(std::numeric_limits<double>::max());
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return v;
            }
            std::string str;
            str.reserve(*col.get_type_length());
            str.append(*col.get_type_length(), 0xFF);
            return Value(str);
        }
        case Value::Type::DATE : {
            Date d;
            d.year = std::numeric_limits<int16_t>::max();
            d.month = 12;
            d.day = 31;
            return Value(d);
        }
        case Value::Type::DATETIME : {
            DateTime dt;
            dt.year = std::numeric_limits<int16_t>::max();
            dt.month = 12;
            dt.day = 31;
            dt.hour = 24;
            dt.minute = 60;
            dt.sec = 60;
            dt.microsec = std::numeric_limits<int32_t>::max();
            dt.timezone = std::numeric_limits<int32_t>::max();
            return Value(dt);
        }
        default :
            return v;
    }
}

Value OptimizerUtils::boundValueWithMin(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = toValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::min());
        }
        case Value::Type::FLOAT : {
            return Value(std::numeric_limits<double>::min());
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return Value(NullType::__NULL__);
            }
            std::string str;
            str.reserve(*col.get_type_length());
            str.append(*col.get_type_length(), '\0');
            return Value(str);
        }
        case Value::Type::DATE : {
            return Value(Date());
        }
        case Value::Type::DATETIME : {
            return Value(DateTime());
        }
        default :
            return Value(NullType::__NULL__);
    }
}

}  // namespace graph
}  // namespace nebula
