/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/FetchVerticesValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

GraphStatus FetchVerticesValidator::validateImpl() {
    auto gStatus = check();
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = prepareVertices();
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = prepareProperties();
    if (!gStatus.ok()) {
        return gStatus;
    }

    return GraphStatus::OK();
}

GraphStatus FetchVerticesValidator::toPlan() {
    // Start [-> some input] -> GetVertices [-> Project] [-> Dedup] [-> next stage] -> End
    std::string vidsVar = (srcRef_ == nullptr ? buildConstantInput() : buildRuntimeInput());
    auto *getVerticesNode = GetVertices::make(qctx_,
                                              nullptr,
                                              spaceId_,
                                              src_,
                                              std::move(props_),
                                              std::move(exprs_),
                                              dedup_,
                                              std::move(orderBy_),
                                              limit_,
                                              std::move(filter_));
    getVerticesNode->setInputVar(vidsVar);
    getVerticesNode->setColNames(std::move(gvColNames_));
    // pipe will set the input variable
    PlanNode *current = getVerticesNode;

    if (withProject_) {
        auto *projectNode = Project::make(qctx_, current, newYieldColumns_);
        projectNode->setInputVar(current->varName());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }
    // Project select properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(qctx_, current);
        dedupNode->setInputVar(current->varName());
        dedupNode->setColNames(colNames_);
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result is required
    }
    root_ = current;
    tail_ = getVerticesNode;
    return GraphStatus::OK();
}

GraphStatus FetchVerticesValidator::check() {
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    spaceId_ = vctx_->whichSpace().id;

    tagName_ = *sentence->tag();
    if (!sentence->isAllTagProps()) {
        tagName_ = *(sentence->tag());
        auto tagStatus = qctx_->schemaMng()->toTagID(spaceId_, tagName_);
        if (!tagStatus.ok()) {
            return GraphStatus::setTagNotFound(tagName_);
        }

        tagId_ = tagStatus.value();
        schema_ = qctx_->schemaMng()->getTagSchema(spaceId_, tagId_.value());
        if (schema_ == nullptr) {
            LOG(ERROR) << "No schema found for " << tagName_;
            return GraphStatus::setTagNotFound(tagName_);
        }
    }
    return GraphStatus::OK();
}

GraphStatus FetchVerticesValidator::prepareVertices() {
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    // from ref, eval when execute
    if (sentence->isRef()) {
        srcRef_ = sentence->ref();
        auto result = checkRef(srcRef_, Value::Type::STRING);
        if (!result.ok()) {
            return GraphStatus::setInvalidExpr(srcRef_->toString());
        }
        inputVar_ = std::move(result).value();
        return GraphStatus::OK();
    }

    // from constant, eval now
    // TODO(shylock) add eval() method for expression
    QueryExpressionContext dummy(nullptr);
    auto vids = sentence->vidList();
    srcVids_.rows.reserve(vids.size());
    for (const auto vid : vids) {
        DCHECK(ExpressionUtils::isConstExpr(vid));
        auto v = vid->eval(dummy);
        if (!v.isStr()) {   // string as vid
            return GraphStatus::setInvalidVid();
        }
        srcVids_.emplace_back(nebula::Row({std::move(v).getStr()}));
    }
    return GraphStatus::OK();
}

// TODO(shylock) select _vid property instead of return always.
<<<<<<< HEAD
Status FetchVerticesValidator::prepareProperties() {
    static constexpr char VertexID[] = "VertexID";
=======
GraphStatus FetchVerticesValidator::prepareProperties() {
>>>>>>> all use GraphStatus
    auto *sentence = static_cast<FetchVerticesSentence*>(sentence_);
    auto *yield = sentence->yieldClause();
    if (yield == nullptr) {
        // empty for all tag and properties
        props_.clear();
        if (!sentence->isAllTagProps()) {
            // for one tag all properties
            storage::cpp2::VertexProp prop;
            prop.set_tag(tagId_.value());
            // empty for all
            props_.emplace_back(std::move(prop));
            outputs_.emplace_back(VertexID, Value::Type::STRING);
            colNames_.emplace_back(VertexID);
            gvColNames_.emplace_back(VertexID);  // keep compatible with 1.0
            for (std::size_t i = 0; i < schema_->getNumFields(); ++i) {
                outputs_.emplace_back(schema_->getFieldName(i),
                                      SchemaUtil::propTypeToValueType(schema_->getFieldType(i)));
                colNames_.emplace_back(tagName_ + '.' + schema_->getFieldName(i));
                gvColNames_.emplace_back(colNames_.back());
            }
        } else {
            // all schema properties
            const auto allTagsResult = qctx_->schemaMng()->getAllVerTagSchema(spaceId_);
            if (!allTagsResult.ok()) {
                return GraphStatus::setNoTags();
            }
            const auto allTags = std::move(allTagsResult).value();
            std::vector<std::pair<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>>
                allTagsSchema;
            allTagsSchema.reserve(allTags.size());
            for (const auto &tag : allTags) {
                allTagsSchema.emplace_back(tag.first, tag.second.back());
            }
            std::sort(allTagsSchema.begin(), allTagsSchema.end(),
                    [](const auto &a, const auto &b) {
                return a.first < b.first;
            });
            outputs_.emplace_back(VertexID, Value::Type::STRING);
            colNames_.emplace_back(VertexID);
            gvColNames_.emplace_back(colNames_.back());
            for (const auto &tagSchema : allTagsSchema) {
                auto tagNameResult = qctx_->schemaMng()->toTagName(spaceId_, tagSchema.first);
                if (!tagNameResult.ok()) {
                    return GraphStatus::setTagNotFound(
                            folly::stringPrintf("TAG_ID: %d", tagSchema.first));
                }
                auto tagName = std::move(tagNameResult).value();
                for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
                    outputs_.emplace_back(
                        tagSchema.second->getFieldName(i),
                        SchemaUtil::propTypeToValueType(tagSchema.second->getFieldType(i)));
                    colNames_.emplace_back(tagName + "." + tagSchema.second->getFieldName(i));
                    gvColNames_.emplace_back(colNames_.back());
                }
            }
        }
    } else {
        CHECK(!sentence->isAllTagProps()) << "Not supported yield for *.";
        withProject_ = true;
        // outputs
        auto yieldSize = yield->columns().size();
        colNames_.reserve(yieldSize + 1);
        outputs_.reserve(yieldSize + 1);
        colNames_.emplace_back(VertexID);
        gvColNames_.emplace_back(colNames_.back());
        outputs_.emplace_back(VertexID, Value::Type::STRING);  // kVid

        dedup_ = yield->isDistinct();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId_.value());
        std::vector<std::string> propsName;
        propsName.reserve(yield->columns().size());
        for (auto col : yield->columns()) {
            if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
                auto laExpr = static_cast<LabelAttributeExpression*>(col->expr());
                col->setExpr(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(
                    laExpr));
            } else {
                ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(col->expr());
            }
            const auto *invalidExpr = findInvalidYieldExpression(col->expr());
            if (invalidExpr != nullptr) {
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("Invalid yield expression `%s'.",
                                             col->expr()->toString().c_str()));
            }
            // The properties from storage directly push down only
            // The other will be computed in Project Executor
            const auto storageExprs = ExpressionUtils::findAllStorage(col->expr());
            for (const auto &storageExpr : storageExprs) {
                const auto *expr = static_cast<const PropertyExpression *>(storageExpr);
                if (*expr->sym() != tagName_) {
                    return GraphStatus::setInvalidExpr(expr->toString());
                }
                // Check is prop name in schema
                if (schema_->getFieldIndex(*expr->prop()) < 0) {
                    LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in tag `"
                                << tagName_ << "'.";
                    return GraphStatus::setColumnNotFound(*expr->prop());
                }
                propsName.emplace_back(*expr->prop());
                gvColNames_.emplace_back(*expr->sym() + "." + *expr->prop());
            }
            colNames_.emplace_back(deduceColName(col));
            auto typeResult = deduceExprType(col->expr());
            if (!typeResult.ok()) {
                return GraphStatus::setInvalidExpr(col->expr()->toString());
            }
            outputs_.emplace_back(colNames_.back(), typeResult.value());
            // TODO(shylock) think about the push-down expr
        }
        prop.set_props(std::move(propsName));
        props_.emplace_back(std::move(prop));

        // insert the reserved properties expression be compatible with 1.0
        // TODO(shylock) select kVid from storage
        newYieldColumns_ = qctx_->objPool()->add(new YieldColumns());
        // note eval vid by input expression
        newYieldColumns_->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(VertexID)),
                            new std::string(VertexID)));
        for (auto col : yield->columns()) {
            newYieldColumns_->addColumn(col->clone().release());
        }
    }

    return GraphStatus::OK();
}

/*static*/
const Expression *FetchVerticesValidator::findInvalidYieldExpression(const Expression *root) {
    return ExpressionUtils::findAnyKind(root,
                                        {Expression::Kind::kInputProperty,
                                         Expression::Kind::kVarProperty,
                                         Expression::Kind::kSrcProperty,
                                         Expression::Kind::kDstProperty,
                                         Expression::Kind::kEdgeSrc,
                                         Expression::Kind::kEdgeType,
                                         Expression::Kind::kEdgeRank,
                                         Expression::Kind::kEdgeDst});
}

// TODO(shylock) optimize dedup input when distinct given
std::string FetchVerticesValidator::buildConstantInput() {
    auto input = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(srcVids_))).finish());

    src_ = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                    new std::string(kVid));
    return input;
}

std::string FetchVerticesValidator::buildRuntimeInput() {
    src_ = DCHECK_NOTNULL(srcRef_);
    return inputVar_;
}

}   // namespace graph
}   // namespace nebula
