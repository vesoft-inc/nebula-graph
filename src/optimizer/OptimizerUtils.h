/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
#define NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_

#include "common/base/Base.h"
#include <common/interface/gen-cpp2/meta_types.h>

namespace nebula {
namespace graph {

class OptimizerUtils {
public:
    enum class BoundValueType {
        GREATER_THAN = 0,
        LESS_THAN,
        MAX,
        MIN,
    };

    // double default precision is 6
    struct DoubleFormat {
        char sign = '+';
        int8_t integral;
        int32_t decimal;
        char exponSign = '+';
        int32_t exponent;

        explicit DoubleFormat(const double& d) {
            size_t offset = 0;
            auto v = folly::stringPrintf("%E", d);
            if (v[0] == '-') {
                sign = '-';
                offset += 1;
            }
            integral = std::stoi(v.substr(offset, offset + 1));
            auto pos = v.find_first_of('E');
            decimal = std::stoi(v.substr(offset + 2, pos - 1));
            if (v[pos + 1] == '-') {
                exponSign = '-';
            }
            exponent = std::abs(std::stoi(v.substr(pos + 1, v.size())));
        }

        double toDouble() {
            auto v = folly::stringPrintf("%d.%-6dE%c%02d", integral, decimal, exponSign, exponent);
            if (sign == '-') {
                v = sign + v;
            }
            return std::strtod(v.c_str(), nullptr);
        }

        bool isPositive() {
            return sign == '+';
        }

        bool isPositiveExpon() {
            return exponSign == '+';
        }
    };

public:
    OptimizerUtils() = delete;

    static Value boundDoubleWithGT(DoubleFormat df);

    static Value boundDoubleWithLT(DoubleFormat df);

    static Value doubleWithGT(DoubleFormat df);

    static Value doubleWithLT(DoubleFormat df);

    static Value boundValue(const meta::cpp2::ColumnDef& col,
                            BoundValueType op,
                            const Value& v = Value());

    static Value boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithMax(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithMin(const meta::cpp2::ColumnDef& col, const Value& v);
};

}
}
#endif  // NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
