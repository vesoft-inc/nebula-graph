/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchEdgesValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

/*static*/ const std::unordered_set<std::string> FetchEdgesValidator::reservedProperties{
    kSrc,
    kType,
    kRank,
    kDst,
};

Status FetchEdgesValidator::validateImpl() {
    NG_RETURN_IF_ERROR(check());
    NG_RETURN_IF_ERROR(prepareEdges());
    NG_RETURN_IF_ERROR(prepareProperties());
    return Status::OK();
}

Status FetchEdgesValidator::toPlan() {
    // Start [-> some input] -> GetEdges [-> Project] [-> Dedup] [-> next stage] -> End
    auto *plan = qctx_->plan();
    auto *doNode = GetEdges::make(plan,
                                  nullptr,
                                  spaceId_,
                                  std::move(edges_),
                                  std::move(src_),
                                  edgeType_,
                                  std::move(ranking_),
                                  std::move(dst_),
                                  std::move(props_),
                                  std::move(exprs_),
                                  dedup_,
                                  limit_,
                                  std::move(orderBy_),
                                  std::move(filter_));
    PlanNode *current = doNode;
    // the framework need to set the input var

    if (withProject_) {
        auto *projectNode = Project::make(plan, current, newYield_->yields());
        projectNode->setInputVar(current->varName());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }
    // Project select the properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current);
        dedupNode->setInputVar(current->varName());
        dedupNode->setColNames(colNames_);
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result is required
    }
    root_ = current;
    tail_ = doNode;
    return Status::OK();
}

Status FetchEdgesValidator::check() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    edgeTypeName_ = *sentence->edge();
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, edgeTypeName_);
    NG_RETURN_IF_ERROR(edgeStatus);
    edgeType_ = edgeStatus.value();
    schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence->edge();
        return Status::Error("No schema found for `%s'", sentence->edge()->c_str());
    }

    return Status::OK();
}

Status FetchEdgesValidator::prepareEdges() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    // from ref, eval in execute
    if (sentence->isRef()) {
        src_ = sentence->ref()->srcid();
        NG_RETURN_IF_ERROR(checkRef(src_, Value::Type::STRING));
        ranking_ = sentence->ref()->rank();
        if (ranking_ == nullptr) {
            // Default zero if ranking not specified
            ranking_ = qctx_->objPool()->add(new ConstantExpression(0));
        } else {
            NG_RETURN_IF_ERROR(checkRef(ranking_, Value::Type::INT));
        }
        dst_ = sentence->ref()->dstid();
        NG_RETURN_IF_ERROR(checkRef(dst_, Value::Type::STRING));
    }

    // from constant, eval now
    QueryExpressionContext dummy(nullptr);
    auto keysPointer = sentence->keys();
    if (keysPointer != nullptr) {
        auto keys = keysPointer->keys();
        // row: _src, _type, _ranking, _dst
        edges_.reserve(keys.size());
        for (const auto &key : keys) {
            DCHECK(ExpressionUtils::isConstExpr(key->srcid()));
            // TODO(shylock) Add new value type EDGE_ID to semantic and simplify this
            auto src = key->srcid()->eval(dummy);
            if (!src.isStr()) {   // string as vid
                return Status::NotSupported("src is not a vertex id");
            }
            auto ranking = key->rank();
            DCHECK(ExpressionUtils::isConstExpr(key->dstid()));
            auto dst = key->dstid()->eval(dummy);
            if (!src.isStr()) {
                return Status::NotSupported("dst is not a vertex id");
            }
            edges_.emplace_back(nebula::Row(
                {std::move(src).getStr(), edgeType_, ranking, std::move(dst).getStr()}));
        }
    }
    return Status::OK();
}

Status FetchEdgesValidator::prepareProperties() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    auto *yield = sentence->yieldClause();
    storage::cpp2::EdgeProp prop;
    prop.set_type(edgeType_);
    // empty for all properties
    if (yield != nullptr) {
        // insert the reserved properties expression be compatible with 1.0
        auto *newYieldColumns = new YieldColumns();
        newYieldColumns->addColumn(
            new YieldColumn(new EdgeSrcIdExpression(new std::string(edgeTypeName_))));
        newYieldColumns->addColumn(
            new YieldColumn(new EdgeDstIdExpression(new std::string(edgeTypeName_))));
        newYieldColumns->addColumn(
            new YieldColumn(new EdgeRankExpression(new std::string(edgeTypeName_))));
        for (auto col : yield->columns()) {
            newYieldColumns->addColumn(col->clone().release());
        }
        newYield_ = qctx_->objPool()->add(new YieldClause(newYieldColumns, yield->isDistinct()));

        std::vector<std::string> propsName;
        propsName.reserve(newYield_->columns().size());
        dedup_ = newYield_->isDistinct();
        exprs_.reserve(newYield_->columns().size());
        for (auto col : newYield_->columns()) {
            if (col->expr()->kind() == Expression::Kind::kSymProperty) {
                auto symbolExpr = static_cast<SymbolPropertyExpression *>(col->expr());
                col->setExpr(ExpressionUtils::transSymbolPropertyExpression<EdgePropertyExpression>(
                    symbolExpr));
            } else {
                ExpressionUtils::transAllSymbolPropertyExpr<EdgePropertyExpression>(col->expr());
            }
            const auto *invalidExpr = findInvalidYieldExpression(col->expr());
            if (invalidExpr != nullptr) {
                return Status::Error("Invalid newYield_ expression `%s'.",
                                     col->expr()->toString().c_str());
            }
            // The properties from storage directly push down only
            // The other will be computed in Project Executor
            const auto storageExprs = ExpressionUtils::findAllStorage(col->expr());
            if (!storageExprs.empty()) {
                if (storageExprs.size() == 1 && ExpressionUtils::isStorage(col->expr())) {
                    // only one expression it's storage property expression
                } else {
                    // need computation in project when storage not do it.
                    withProject_ = true;
                }
                for (const auto &storageExpr : storageExprs) {
                    const auto *expr = static_cast<const SymbolPropertyExpression *>(storageExpr);
                    if (*expr->sym() != edgeTypeName_) {
                        return Status::Error("Mismatched edge type name");
                    }
                    // Check is prop name in schema
                    if (schema_->getFieldIndex(*expr->prop()) < 0 &&
                        reservedProperties.find(*expr->prop()) == reservedProperties.end()) {
                        LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in edge `"
                                   << edgeTypeName_ << "'.";
                        return Status::Error("Unknown column `%s' in edge `%s'",
                                             expr->prop()->c_str(),
                                             edgeTypeName_.c_str());
                    }
                    propsName.emplace_back(*expr->prop());
                }
                storage::cpp2::Expr exprAlias;
                if (col->alias()) {
                    exprAlias.set_alias(*col->alias());
                }
                exprAlias.set_expr(col->expr()->encode());
                exprs_.emplace_back(std::move(exprAlias));
            } else {
                // Need project to evaluate the expression not push down to storage
                // And combine the result from storage
                withProject_ = true;
            }
        }
        prop.set_props(std::move(propsName));

        // outpus
        colNames_ = deduceColNames(newYield_->yields());
        outputs_.reserve(colNames_.size());
        for (std::size_t i = 0; i < colNames_.size(); ++i) {
            auto typeResult = deduceExprType(newYield_->columns()[i]->expr());
            NG_RETURN_IF_ERROR(typeResult);
            outputs_.emplace_back(colNames_[i], typeResult.value());
        }
    } else {
        // no yield
        std::vector<std::string> propNames;   // filter the type
        propNames.reserve(3 + schema_->getNumFields());
        outputs_.reserve(3 + schema_->getNumFields());
        colNames_.reserve(3 + schema_->getNumFields());
        // insert the reserved properties be compatible with 1.0
        propNames.emplace_back(kSrc);
        outputs_.emplace_back(edgeTypeName_ + "." + kSrc, Value::Type::STRING);
        colNames_.emplace_back(edgeTypeName_ + "." + kSrc);
        propNames.emplace_back(kDst);
        outputs_.emplace_back(edgeTypeName_ + "." + kDst, Value::Type::STRING);
        colNames_.emplace_back(edgeTypeName_ + "." + kDst);
        propNames.emplace_back(kRank);
        outputs_.emplace_back(edgeTypeName_ + "." + kRank, Value::Type::INT);
        colNames_.emplace_back(edgeTypeName_ + "." + kRank);

        for (std::size_t i = 0; i < schema_->getNumFields(); ++i) {
            propNames.emplace_back(schema_->getFieldName(i));
            outputs_.emplace_back(schema_->getFieldName(i),
                                  SchemaUtil::propTypeToValueType(schema_->getFieldType(i)));
            colNames_.emplace_back(schema_->getFieldName(i));
        }
        prop.set_props(std::move(propNames));
    }

    props_.emplace_back(std::move(prop));
    return Status::OK();
}

/*static*/
const Expression *FetchEdgesValidator::findInvalidYieldExpression(const Expression *root) {
    return ExpressionUtils::findAnyKind(root,
                                        Expression::Kind::kInputProperty,
                                        Expression::Kind::kVarProperty,
                                        Expression::Kind::kSrcProperty,
                                        Expression::Kind::kDstProperty);
}

}   // namespace graph
}   // namespace nebula
