/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VISITOR_CHECKEXPREVALUABLEVISITOR_H_
#define VALIDATOR_VISITOR_CHECKEXPREVALUABLEVISITOR_H_

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class CheckExprEvaluableVisitor : public ExprVisitorImpl {
public:
    bool ok() const override {
        return isEvaluable_;
    }

private:
    void visitConstantExpr(ConstantExpression*) override {
        isEvaluable_ = true;
    }

    bool isEvaluable_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_VISITOR_CHECKEXPREVALUABLEVISITOR_H_
