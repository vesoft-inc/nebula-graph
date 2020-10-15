/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/IndexScanValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

Status IndexScanValidator::validateImpl() {
    NG_RETURN_IF_ERROR(prepareFrom());
    NG_RETURN_IF_ERROR(prepareYield());
    NG_RETURN_IF_ERROR(prepareFilter());
    return Status::OK();
}

Status IndexScanValidator::toPlan() {
    return genSingleNodePlan<IndexScan>(spaceId_,
                                        std::move(contexts_),
                                        std::move(returnCols_),
                                        isEdge_,
                                        schemaId_);
}

Status IndexScanValidator::prepareFrom() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    const auto* from = sentence->from();
    auto ret = qctx_->schemaMng()->getSchemaIDByName(spaceId_, *from);
    if (!ret.ok()) {
        return ret.status();
    }
    isEdge_ = ret.value().first;
    schemaId_ = ret.value().second;
    return Status::OK();
}

Status IndexScanValidator::prepareYield() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    returnCols_ = std::make_unique<std::vector<std::string>>();
    if (sentence->yieldClause() == nullptr) {
        return Status::OK();
    }
    auto columns = sentence->yieldClause()->columns();
    auto schema = isEdge_
                  ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                  : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    const auto* from = sentence->from();
    if (schema == nullptr) {
        return isEdge_
               ? Status::EdgeNotFound("Edge schema not found : %s", from->c_str())
               : Status::TagNotFound("Tag schema not found : %s", from->c_str());
    }
    for (auto col : columns) {
        std::string schemaName, colName;
        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<LabelAttributeExpression *>(col->expr());
            schemaName = *la->left()->name();
            colName = *la->right()->name();
        } else {
            return Status::SemanticError("Yield clauses are not supported : %s",
                                         col->expr()->toString().c_str());
        }

        if (schemaName != *from) {
            return Status::SemanticError("Schema name error : %s", schemaName.c_str());
        }
        auto ret = schema->getFieldType(colName);
        if (ret == meta::cpp2::PropertyType::UNKNOWN) {
            return Status::SemanticError("Column %s not found in schema %s",
                                         colName.c_str(), from->c_str());
        }
        returnCols_->emplace_back(colName);
    }
    return Status::OK();
}

Status IndexScanValidator::prepareFilter() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    auto *filter = sentence->whereClause()->filter();
    auto ret = checkFilter(filter, *sentence->from());
    NG_RETURN_IF_ERROR(ret);
    storage::cpp2::IndexQueryContext ctx;
    ctx.set_filter(Expression::encode(*filter));
    contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    contexts_->emplace_back(std::move(ctx));
    return Status::OK();
}

Status IndexScanValidator::checkFilter(Expression* expr, const std::string& from) {
    // TODO (sky) : Rewrite simple expressions,
    //              for example rewrite expr from col1 > 1 + 2 to col > 3
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = static_cast<LogicalExpression*>(expr);
            auto ret = checkFilter(lExpr->left(), from);
            NG_RETURN_IF_ERROR(ret);
            ret = checkFilter(lExpr->right(), from);
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelNE: {
            auto* rExpr = static_cast<RelationalExpression*>(expr);
            return checkRelExpr(rExpr, from);
        }
        default: {
            return Status::NotSupported("Expression %s not supported yet",
                                        expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status IndexScanValidator::checkRelExpr(RelationalExpression* expr,
                                        const std::string& from) {
    auto* left = expr->left();
    auto* right = expr->right();
    // Does not support filter : schema.col1 > schema.col2
    if (left->kind() == Expression::Kind::kLabelAttribute &&
        right->kind() == Expression::Kind::kLabelAttribute) {
        return Status::NotSupported("Expression %s not supported yet",
                                    expr->toString().c_str());
    } else if (left->kind() == Expression::Kind::kLabelAttribute ||
               right->kind() == Expression::Kind::kLabelAttribute) {
        auto ret = rewriteRelExpr(expr, from);
        NG_RETURN_IF_ERROR(ret);
    } else {
        return Status::NotSupported("Expression %s not supported yet",
                                    expr->toString().c_str());
    }
    return Status::OK();
}

Status IndexScanValidator::rewriteRelExpr(RelationalExpression* expr,
                                          const std::string& from) {
    auto* left = expr->left();
    auto* right = expr->right();
    auto leftIsAE = left->kind() == Expression::Kind::kLabelAttribute;

    std::string ref, prop;
    auto* la = leftIsAE
               ? static_cast<LabelAttributeExpression *>(left)
               : static_cast<LabelAttributeExpression *>(right);
    if (*la->left()->name() != from) {
        return Status::SemanticError("Schema name error : %s",
                                     la->left()->name()->c_str());
    }

    ref = *la->left()->name();
    prop = *la->right()->name();

    // rewrite ConstantExpression
    auto c = leftIsAE
             ? checkConstExpr(right, prop)
             : checkConstExpr(left, prop);

    if (!c.ok()) {
        return Status::SemanticError("expression error : %s", left->toString().c_str());
    }

    if (leftIsAE) {
        expr->setRight(new ConstantExpression(std::move(c).value()));
    } else {
        expr->setLeft(new ConstantExpression(std::move(c).value()));
    }

    // rewrite PropertyExpression
    if (leftIsAE) {
        if (isEdge_) {
            expr->setLeft(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(la));
        } else {
            expr->setLeft(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(la));
        }
    } else {
        if (isEdge_) {
            expr->setRight(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(la));
        } else {
            expr->setRight(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(la));
        }
    }
    return Status::OK();
}

StatusOr<Value> IndexScanValidator::checkConstExpr(Expression* expr,
                                                   const std::string& prop) {
    auto schema = isEdge_
                  ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                  : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    auto type = schema->getFieldType(prop);
    QueryExpressionContext dummy(nullptr);
    auto v = Expression::eval(expr, dummy);
    if (v.type() != SchemaUtil::propTypeToValueType(type)) {
        return Status::SemanticError("Column type error : %s", prop.c_str());
    }
    return v;
}

}   // namespace graph
}   // namespace nebula
