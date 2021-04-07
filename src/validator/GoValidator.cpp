/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GoValidator.h"

#include "util/ExpressionUtils.h"

#include "common/base/Base.h"
#include "common/expression/VariableExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Logic.h"
#include "visitor/ExtractPropExprVisitor.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    goCtx_ = getContext<GoAstContext>();
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateStep(goSentence->stepClause(), goCtx_->steps));
    NG_RETURN_IF_ERROR(validateStarts(goSentence->fromClause(), goCtx_->from));
    NG_RETURN_IF_ERROR(validateOver(goSentence->overClause(), goCtx_->over));
    NG_RETURN_IF_ERROR(validateWhere(goSentence->whereClause()));
    NG_RETURN_IF_ERROR(validateYield(goSentence->yieldClause()));

    if (!goCtx_->exprProps.inputProps().empty() && goCtx_->from.fromType != kPipe) {
        return Status::SemanticError("$- must be referred in FROM before used in WHERE or YIELD");
    }

    if (!goCtx_->exprProps.varProps().empty() && goCtx_->from.fromType != kVariable) {
        return Status::SemanticError(
            "A variable must be referred in FROM before used in WHERE or YIELD");
    }

    if ((!goCtx_->exprProps.inputProps().empty() && !goCtx_->exprProps.varProps().empty()) ||
        goCtx_->exprProps.varProps().size() > 1) {
        return Status::SemanticError("Only support single input in a go sentence.");
    }

    NG_RETURN_IF_ERROR(buildColumns());

    goCtx_->inputVarName = inputVarName_;

    return Status::OK();
}

Status GoValidator::validateWhere(WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    goCtx_->filter = where->filter();
    if (graph::ExpressionUtils::findAny(goCtx_->filter, {Expression::Kind::kAggregate})) {
        return Status::SemanticError(
            "`%s', not support aggregate function in where sentence.",
            goCtx_->filter->toString().c_str());
    }
    goCtx_->filter = ExpressionUtils::rewriteLabelAttr2EdgeProp(goCtx_->filter);

    auto typeStatus = deduceExprType(goCtx_->filter);
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
        type != Value::Type::__EMPTY__) {
        std::stringstream ss;
        ss << "`" << goCtx_->filter->toString() << "', expected Boolean, "
           << "but was `" << type << "'";
        return Status::SemanticError(ss.str());
    }

    NG_RETURN_IF_ERROR(deduceProps(goCtx_->filter, goCtx_->exprProps));
    return Status::OK();
}

Status GoValidator::validateYield(YieldClause* yield) {
    if (yield == nullptr) {
        return Status::SemanticError("Yield clause nullptr.");
    }

    goCtx_->distinct = yield->isDistinct();
    auto cols = yield->columns();

    if (cols.empty() && goCtx_->over.isOverAll) {
        DCHECK(!goCtx_->over.allEdges.empty());
        auto* newCols = new YieldColumns();
        qctx_->objPool()->add(newCols);
        for (auto& e : goCtx_->over.allEdges) {
            auto* col = new YieldColumn(new EdgeDstIdExpression(new std::string(e)));
            newCols->addColumn(col);
            auto colName = deduceColName(col);
            goCtx_->colNames.emplace_back(colName);
            outputs_.emplace_back(colName, vidType_);
            NG_RETURN_IF_ERROR(deduceProps(col->expr(), goCtx_->exprProps));
        }

        goCtx_->yields = newCols;
    } else {
        for (auto col : cols) {
            col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));
            NG_RETURN_IF_ERROR(invalidLabelIdentifiers(col->expr()));

            auto* colExpr = col->expr();
            if (graph::ExpressionUtils::findAny(colExpr, {Expression::Kind::kAggregate})) {
                return Status::SemanticError("`%s', not support aggregate function in go sentence.",
                                             col->toString().c_str());
            }
            auto colName = deduceColName(col);
            goCtx_->colNames.emplace_back(colName);
            // check input var expression
            auto typeStatus = deduceExprType(colExpr);
            NG_RETURN_IF_ERROR(typeStatus);
            auto type = typeStatus.value();
            outputs_.emplace_back(colName, type);

            NG_RETURN_IF_ERROR(deduceProps(colExpr, goCtx_->exprProps));
        }
        for (auto& e : goCtx_->exprProps.edgeProps()) {
            auto found =
                std::find(goCtx_->over.edgeTypes.begin(), goCtx_->over.edgeTypes.end(), e.first);
            if (found == goCtx_->over.edgeTypes.end()) {
                return Status::SemanticError("Edges should be declared first in over clause.");
            }
        }
        goCtx_->yields = yield->yields();
    }
    return Status::OK();
}

void GoValidator::extractPropExprs(const Expression* expr) {
    ExtractPropExprVisitor visitor(vctx_,
                                   goCtx_->srcAndEdgePropCols,
                                   goCtx_->dstPropCols,
                                   goCtx_->inputPropCols,
                                   goCtx_->propExprColMap);
    const_cast<Expression*>(expr)->accept(&visitor);
}

Expression* GoValidator::rewriteToInputProp(const Expression* expr) {
    auto matcher = [this](const Expression* e) -> bool {
        return goCtx_->propExprColMap.find(e->toString()) != goCtx_->propExprColMap.end();
    };
    auto rewriter = [this](const Expression* e) -> Expression* {
        auto iter = goCtx_->propExprColMap.find(e->toString());
        DCHECK(iter != goCtx_->propExprColMap.end());
        return new InputPropertyExpression(new std::string(*(iter->second->alias())));
    };

    return RewriteVisitor::transform(expr, matcher, rewriter);
}

Status GoValidator::buildColumns() {
    if (goCtx_->exprProps.dstTagProps().empty() && goCtx_->exprProps.inputProps().empty() &&
        goCtx_->exprProps.varProps().empty() && goCtx_->from.fromType == FromType::kInstantExpr) {
        return Status::OK();
    }

    auto pool = qctx_->objPool();
    if (!goCtx_->exprProps.isAllPropsEmpty() || goCtx_->from.fromType != FromType::kInstantExpr) {
        goCtx_->srcAndEdgePropCols = pool->add(new YieldColumns());
    }

    if (!goCtx_->exprProps.dstTagProps().empty()) {
        goCtx_->dstPropCols = pool->add(new YieldColumns());
    }

    if (!goCtx_->exprProps.inputProps().empty() || !goCtx_->exprProps.varProps().empty()) {
        goCtx_->inputPropCols = pool->add(new YieldColumns());
    }

    if (goCtx_->filter != nullptr) {
        extractPropExprs(goCtx_->filter);
        auto newFilter = goCtx_->filter->clone();
        goCtx_->newFilter = rewriteToInputProp(newFilter.get());
        pool->add(goCtx_->newFilter);
    }

    goCtx_->newYieldCols = pool->add(new YieldColumns());
    for (auto* yield : goCtx_->yields->columns()) {
        extractPropExprs(yield->expr());
        auto* alias = yield->alias() == nullptr ? nullptr : new std::string(*(yield->alias()));
        goCtx_->newYieldCols->addColumn(new YieldColumn(rewriteToInputProp(yield->expr()), alias));
    }

    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
