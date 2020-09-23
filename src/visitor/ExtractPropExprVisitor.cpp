/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ExtractPropExprVisitor.h"

namespace nebula {
namespace graph {

void ExtractPropExprVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
}

// case Expression::Kind::kUUID:
//         case Expression::Kind::kVar:
//         case Expression::Kind::kVersionedVar:
//         case Expression::Kind::kUnaryIncr:
//         case Expression::Kind::kUnaryDecr:
//         case Expression::Kind::kSubscript:
//         case Expression::Kind::kAttribute:
//         case Expression::Kind::kLabelAttribute:
//         case Expression::Kind::kLabel:
}   // namespace graph
}   // namespace nebula
