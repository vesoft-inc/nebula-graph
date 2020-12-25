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
    std::string edgeKeysVar = (srcRef_ == nullptr ? buildConstantInput() : buildRuntimeInput());

    auto *getEdgesNode = GetEdges::make(qctx_,
                                        nullptr,
                                        spaceId_,
                                        src_,
                                        type_,
                                        rank_,
                                        dst_,
                                        std::move(props_),
                                        std::move(exprs_),
                                        dedup_,
                                        limit_,
                                        std::move(orderBy_),
                                        std::move(filter_));
    getEdgesNode->setInputVar(edgeKeysVar);
    getEdgesNode->setColNames(geColNames_);
    // the pipe will set the input variable
    PlanNode *current = getEdgesNode;

    // filter when the edge key is empty which means not exists edge in fact
    auto *notExistEdgeFilter = Filter::make(qctx_, current, emptyEdgeKeyFilter());
    notExistEdgeFilter->setInputVar(current->outputVar());
    notExistEdgeFilter->setColNames(geColNames_);
    current = notExistEdgeFilter;

    if (withProject_) {
        auto *projectNode = Project::make(qctx_, current, newYield_->yields());
        projectNode->setInputVar(current->outputVar());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }
    // Project select the properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(qctx_, current);
        dedupNode->setInputVar(current->outputVar());
        dedupNode->setColNames(colNames_);
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result is required
    }
    root_ = current;
    tail_ = getEdgesNode;
    return Status::OK();
}

Status FetchEdgesValidator::check() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    if (sentence->edgeSize() > 1) {
        return Status::SemanticError("Only allow fetch on one edge.");
    }
    spaceId_ = vctx_->whichSpace().id;
    edgeTypeName_ = *sentence->edge();
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, edgeTypeName_);
    NG_RETURN_IF_ERROR(edgeStatus);
    edgeType_ = edgeStatus.value();
    schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence->edge();
        return Status::SemanticError("No schema found for `%s'", sentence->edge()->c_str());
    }

    return Status::OK();
}

Status FetchEdgesValidator::prepareEdges() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    // from ref, eval in execute
    if (sentence->isRef()) {
        srcRef_ = sentence->ref()->srcid();
        auto result = checkRef(srcRef_, vidType_);
        NG_RETURN_IF_ERROR(result);
        inputVar_ = std::move(result).value();
        rankRef_ = sentence->ref()->rank();
        if (rankRef_->kind() != Expression::Kind::kConstant) {
            result = checkRef(rankRef_, Value::Type::INT);
            NG_RETURN_IF_ERROR(result);
            if (inputVar_ != result.value()) {
                return Status::SemanticError(
                    "Can't refer to different variable as key at same time.");
            }
        }
        dstRef_ = sentence->ref()->dstid();
        result = checkRef(dstRef_, vidType_);
        NG_RETURN_IF_ERROR(result);
        if (inputVar_ != result.value()) {
            return Status::SemanticError("Can't refer to different variable as key at same time.");
        }
        return Status::OK();
    }

    // from constant, eval now
    QueryExpressionContext dummy(nullptr);
    auto keysPointer = sentence->keys();
    if (keysPointer != nullptr) {
        auto keys = keysPointer->keys();
        // row: _src, _type, _ranking, _dst
        edgeKeys_.rows.reserve(keys.size());
        for (const auto &key : keys) {
            DCHECK(ExpressionUtils::isConstExpr(key->srcid()));
            auto src = key->srcid()->eval(dummy);
            if (src.type() != vidType_) {
                std::stringstream ss;
                ss << "the src should be type of " << vidType_ << ", but was`" << src.type() << "'";
                return Status::SemanticError(ss.str());
            }
            auto ranking = key->rank();
            DCHECK(ExpressionUtils::isConstExpr(key->dstid()));
            auto dst = key->dstid()->eval(dummy);
            if (dst.type() != vidType_) {
                std::stringstream ss;
                ss << "the dst should be type of " << vidType_ << ", but was`" << dst.type() << "'";
                return Status::SemanticError(ss.str());
            }
            edgeKeys_.emplace_back(nebula::Row({std::move(src), ranking, std::move(dst)}));
        }
    }
    return Status::OK();
}

Status FetchEdgesValidator::prepareProperties() {
    auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
    auto *yield = sentence->yieldClause();
    // empty for all properties
    if (yield != nullptr) {
        return preparePropertiesWithYield(yield);
    } else {
        return preparePropertiesWithoutYield();
    }
}

Status FetchEdgesValidator::preparePropertiesWithYield(const YieldClause *yield) {
    withProject_ = true;
    storage::cpp2::EdgeProp prop;
    prop.set_type(edgeType_);
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

    auto newYieldSize = newYield_->columns().size();
    colNames_.reserve(newYieldSize);
    outputs_.reserve(newYieldSize);

    std::vector<std::string> propsName;
    propsName.reserve(newYield_->columns().size());
    dedup_ = newYield_->isDistinct();
    for (auto col : newYield_->columns()) {
        NG_RETURN_IF_ERROR(invalidLabelIdentifiers(col->expr()));

        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto laExpr = static_cast<LabelAttributeExpression *>(col->expr());
            col->setExpr(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(laExpr));
        } else {
            ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(col->expr());
        }
        const auto *invalidExpr = findInvalidYieldExpression(col->expr());
        if (invalidExpr != nullptr) {
            return Status::SemanticError("Invalid newYield_ expression `%s'.",
                                         col->expr()->toString().c_str());
        }
        // The properties from storage directly push down only
        // The other will be computed in Project Executor
        const auto storageExprs = ExpressionUtils::findAllStorage(col->expr());
        for (const auto &storageExpr : storageExprs) {
            const auto *expr = static_cast<const PropertyExpression *>(storageExpr);
            if (*expr->sym() != edgeTypeName_) {
                return Status::SemanticError("Mismatched edge type name");
            }
            // Check is prop name in schema
            if (schema_->getFieldIndex(*expr->prop()) < 0 &&
                reservedProperties.find(*expr->prop()) == reservedProperties.end()) {
                LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in edge `" << edgeTypeName_
                           << "'.";
                return Status::SemanticError("Unknown column `%s' in edge `%s'",
                                             expr->prop()->c_str(),
                                             edgeTypeName_.c_str());
            }
            propsName.emplace_back(*expr->prop());
            geColNames_.emplace_back(*expr->sym() + "." + *expr->prop());
        }
        colNames_.emplace_back(deduceColName(col));
        auto typeResult = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(typeResult);
        outputs_.emplace_back(colNames_.back(), typeResult.value());
        // TODO(shylock) think about the push-down expr
    }
    prop.set_props(std::move(propsName));
    props_.emplace_back(std::move(prop));
    return Status::OK();
}

Status FetchEdgesValidator::preparePropertiesWithoutYield() {
    // no yield
    storage::cpp2::EdgeProp prop;
    prop.set_type(edgeType_);
    std::vector<std::string> propNames;   // filter the type
    propNames.reserve(3 + schema_->getNumFields());
    outputs_.reserve(3 + schema_->getNumFields());
    colNames_.reserve(3 + schema_->getNumFields());
    geColNames_.reserve(3 + schema_->getNumFields());
    // insert the reserved properties be compatible with 1.0
    propNames.emplace_back(kSrc);
    colNames_.emplace_back(edgeTypeName_ + "." + kSrc);
    outputs_.emplace_back(colNames_.back(), vidType_);
    geColNames_.emplace_back(colNames_.back());
    propNames.emplace_back(kDst);
    colNames_.emplace_back(edgeTypeName_ + "." + kDst);
    outputs_.emplace_back(colNames_.back(), vidType_);
    geColNames_.emplace_back(colNames_.back());
    propNames.emplace_back(kRank);
    colNames_.emplace_back(edgeTypeName_ + "." + kRank);
    outputs_.emplace_back(colNames_.back(), Value::Type::INT);
    geColNames_.emplace_back(colNames_.back());

    for (std::size_t i = 0; i < schema_->getNumFields(); ++i) {
        propNames.emplace_back(schema_->getFieldName(i));
        outputs_.emplace_back(schema_->getFieldName(i),
                              SchemaUtil::propTypeToValueType(schema_->getFieldType(i)));
        colNames_.emplace_back(edgeTypeName_ + "." + schema_->getFieldName(i));
        geColNames_.emplace_back(colNames_.back());
    }
    prop.set_props(std::move(propNames));
    props_.emplace_back(std::move(prop));
    return Status::OK();
}

/*static*/
const Expression *FetchEdgesValidator::findInvalidYieldExpression(const Expression *root) {
    return ExpressionUtils::findAny(root,
                                    {Expression::Kind::kInputProperty,
                                     Expression::Kind::kVarProperty,
                                     Expression::Kind::kSrcProperty,
                                     Expression::Kind::kDstProperty});
}

// TODO(shylock) optimize dedup input when distinct given
std::string FetchEdgesValidator::buildConstantInput() {
    auto pool = qctx_->objPool();
    auto input = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(edgeKeys_))).finish());

    src_ =
        pool->makeAndAdd<VariablePropertyExpression>(new std::string(input), new std::string(kSrc));
    type_ = pool->makeAndAdd<ConstantExpression>(edgeType_);
    rank_ = pool->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                         new std::string(kRank));
    dst_ =
        pool->makeAndAdd<VariablePropertyExpression>(new std::string(input), new std::string(kDst));
    return input;
}

std::string FetchEdgesValidator::buildRuntimeInput() {
    auto pool = qctx_->objPool();
    src_ = DCHECK_NOTNULL(srcRef_);
    type_ = pool->makeAndAdd<ConstantExpression>(edgeType_);
    rank_ = DCHECK_NOTNULL(rankRef_);
    dst_ = DCHECK_NOTNULL(dstRef_);
    return inputVar_;
}

Expression *FetchEdgesValidator::emptyEdgeKeyFilter() {
    // _src != empty && _dst != empty && _rank != empty
    DCHECK_GE(geColNames_.size(), 3);
    auto *srcNotEmptyExpr = notEmpty(new EdgeSrcIdExpression(new std::string(edgeTypeName_)));
    auto *dstNotEmptyExpr = notEmpty(new EdgeDstIdExpression(new std::string(edgeTypeName_)));
    auto *rankNotEmptyExpr = notEmpty(new EdgeRankExpression(new std::string(edgeTypeName_)));
    auto *edgeKeyNotEmptyExpr =
        qctx_->objPool()->add(lgAnd(srcNotEmptyExpr, lgAnd(dstNotEmptyExpr, rankNotEmptyExpr)));
    return edgeKeyNotEmptyExpr;
}

}   // namespace graph
}   // namespace nebula
