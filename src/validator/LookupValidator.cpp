/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/expression/VariableExpression.h"
#include "parser/TraverseSentences.h"
#include "validator/LookupValidator.h"

namespace nebula {
namespace graph {
Status LookupValidator::validateImpl() {
    auto* lookupSentence = static_cast<LookupSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateFrom(lookupSentence->from()));
    NG_RETURN_IF_ERROR(validateWhere(lookupSentence->whereClause()));
    NG_RETURN_IF_ERROR(validateYield(lookupSentence->yieldClause()));
    return Status::OK();
}

Status LookupValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto* gn = Lookup::make(plan, nullptr, space_.id);
    gn->setQueryContext(buildIndexQueryCtx());
    gn->setreturnCols(buildIndexReturnCols());
    gn->setSchemaId(schemaId());
    gn->setIsEdge(isEdgeIndex());
    root_ = gn;
    tail_ = root_;
    return Status::OK();
}

Status LookupValidator::validateFrom(const std::string* from) {
    if (from->empty()) {
        return Status::Error("From clause empty.");
    }
    from_ = *from;
    auto *mc = qctx_->getMetaClient();
    auto tagResult = mc->getTagIDByNameFromCache(space_.id, *from);
    if (tagResult.ok()) {
        schemaId_.set_tag_id(std::move(tagResult).value());
        return Status::OK();
    }
    auto edgeResult = mc->getEdgeTypeByNameFromCache(space_.id, *from);
    if (edgeResult.ok()) {
        schemaId_.set_edge_type(std::move(edgeResult).value());
        return Status::OK();
    }
    return Status::Error("Tag or Edge was not found : %s", from->c_str());
}

bool LookupValidator::isEdgeIndex() {
    return schemaId_.getType() == meta::cpp2::SchemaID::Type::edge_type;
}

int32_t LookupValidator::schemaId() {
    return isEdgeIndex() ? schemaId_.get_edge_type() : schemaId_.get_tag_id();
}

Status LookupValidator::getSchema() {
    auto *sm = qctx_->schemaMng();
    if (sm == nullptr) {
        return Status::Error("Schema Manager error.");
    }
    schema_ = isEdgeIndex()
              ? sm->getEdgeSchema(space_.id, schemaId())
              : sm->getTagSchema(space_.id, schemaId());
    if (schema_ == nullptr) {
        return Status::Error("Schema not found");
    }
    return Status::OK();
}

Status LookupValidator::validateWhere(const WhereClause* where) {
    if (where == nullptr) {
        return Status::Error("Where clause is empty.");
    }
    NG_RETURN_IF_ERROR(getSchema());
    filter_ = where->filter();
    return validateFilter(filter_);
}

Status LookupValidator::validateFilter(const Expression* expr) {
    // TODO : function expression.
    switch (expr->kind()) {
        case Expression::Kind::kConstant : {
            break;
        }
        case Expression::Kind::kLogicalAnd :
        case Expression::Kind::kLogicalOr : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            const auto* left = lExpr->left();
            NG_RETURN_IF_ERROR(validateFilter(left));
            auto* right = lExpr->right();
            NG_RETURN_IF_ERROR(validateFilter(right));
            break;
        }
        case Expression::Kind::kRelNE :
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelLT: {
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            const auto* left = rExpr->left();
            NG_RETURN_IF_ERROR(validateFilter(left));
            auto* right = rExpr->right();
            NG_RETURN_IF_ERROR(validateFilter(right));
            break;
        }
        case Expression::Kind::kSymProperty: {
            auto* sExpr = static_cast<const SymbolPropertyExpression*>(expr);
            NG_RETURN_IF_ERROR(checkPropExpression(sExpr));
            break;
        }
        default : {
            return Status::SyntaxError("Syntax error ： %s", expr->toString().c_str());
        }
    }
  return Status::OK();
}

Status LookupValidator::validateYield(const YieldClause* yield) {
  if (yield == nullptr) {
    return Status::OK();
  }
  for (const auto* col : yield->columns()) {
    if (col->getAggFunName() != nullptr) {
      return Status::SyntaxError("lookup not support aggregate function.");
    }
    if (col->expr() == nullptr) {
      return Status::SyntaxError("Yield clause error");
    }
    // TODO : convert expression form EdgePropertyExpression
    //        to SymbolPropertyExpression through parser layer.
    if (col->expr()->kind() != Expression::Kind::kSymProperty) {
      return Status::SyntaxError("lookup not support expression %s",
                                 col->expr()->toString().c_str());
    }
    auto* expr = static_cast<const SymbolPropertyExpression*>(col->expr());
    NG_RETURN_IF_ERROR(checkPropExpression(expr));
    returnCols_.emplace_back(*expr->prop());
  }
  yields_ = yield->yields();
  return Status::OK();
}

Status LookupValidator::checkPropExpression(const SymbolPropertyExpression* expr) {
    if (from_ != expr->ref()) {
        return Status::Error("No schema found `%s'", from_.c_str());
    }
    if (schema_->getFieldIndex(expr->prop()->c_str()) < 0) {
        return Status::Error("Unknown column `%s' in schema",
                             expr->prop()->c_str());
    }
    return Status::OK();
}

Lookup::IndexQueryCtx LookupValidator::buildIndexQueryCtx() {
    auto contexts = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    storage::cpp2::IndexQueryContext ctx;
    // TODO (sky) : rewrite filter through optimizer rule.
    ctx.set_filter(Expression::encode(*filter_));
    contexts->emplace_back(std::move(ctx));
    return contexts;
}

Lookup::IndexReturnCols LookupValidator::buildIndexReturnCols() {
    auto returnCols = std::make_unique<std::vector<std::string>>();
    for (const auto& col : returnCols_) {
        returnCols->emplace_back(col);
    }
    return returnCols;
}

}  // namespace graph
}  // namespace nebula
