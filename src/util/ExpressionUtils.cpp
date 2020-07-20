/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"
#include "common/expression/BinaryExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"

namespace nebula {
namespace graph {

bool ExpressionUtils::traverse(const Expression* expr, Visitor visitor) {
    // TODO kind name
    DLOG(INFO) << "Expression kind " << static_cast<int>(expr->kind());
    if (!visitor(expr)) {
        return false;
    }
    switch (expr->kind()) {
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kConstant: {
            return true;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            UNUSED(DCHECK_NOTNULL(dynamic_cast<const BinaryExpression*>(expr)));
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            if (!traverse(biExpr->left(), visitor)) {
                return false;
            }
            return traverse(biExpr->right(), visitor);
        }
        case Expression::Kind::kRelIn:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            UNUSED(DCHECK_NOTNULL(dynamic_cast<const UnaryExpression*>(expr)));
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            return traverse(unaryExpr->operand(), visitor);
        }
        case Expression::Kind::kTypeCasting: {
            auto typeCatingExpr = static_cast<const TypeCastingExpression*>(expr);
            return traverse(typeCatingExpr->operand(), visitor);
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression*>(expr);
            for (auto& arg : funcExpr->args()->args()) {
                if (!traverse(arg.get(), visitor)) {
                    return false;
                }
            }
            return true;
        }
    }
    DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
    return false;
}

}   // namespace graph
}   // namespace nebula
