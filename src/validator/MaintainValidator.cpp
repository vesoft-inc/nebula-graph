/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"
#include "common/charset/Charset.h"

#include "util/SchemaUtil.h"
#include "parser/MaintainSentences.h"
#include "service/GraphFlags.h"
#include "planner/Maintain.h"
#include "planner/Query.h"
#include "validator/MaintainValidator.h"

namespace nebula {
namespace graph {
GraphStatus CreateTagValidator::validateImpl() {
    auto sentence = static_cast<CreateTagSentence*>(sentence_);
    auto status = GraphStatus::OK();
    name_ = *sentence->name();
    ifNotExist_ = sentence->isIfNotExist();
    do {
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(name_);
        if (pro != nullptr) {
            status = GraphStatus::setSemanticError(
                    folly::stringPrintf("Has the same name `%s' in the SequentialSentences",
                                         name_.c_str()));
            break;
        }

        status = SchemaUtil::validateColumns(sentence->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(name_, schemaPro);
    }
    return status;
}

GraphStatus CreateTagValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = CreateTag::make(qctx_,
            plan->root(), std::move(name_), std::move(schema_), ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus CreateEdgeValidator::validateImpl() {
    auto sentence = static_cast<CreateEdgeSentence*>(sentence_);
    auto status = GraphStatus::OK();
    name_ = *sentence->name();
    ifNotExist_ = sentence->isIfNotExist();
    do {
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(name_);
        if (pro != nullptr) {
            status = GraphStatus::setSemanticError(
                    folly::stringPrintf("Has the same name `%s' in the SequentialSentences",
                                        name_.c_str()));
            break;
        }

        status = SchemaUtil::validateColumns(sentence->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);

    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(name_, schemaPro);
    }
    return status;
}

GraphStatus CreateEdgeValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = CreateEdge::make(qctx_,
            plan->root(), std::move(name_), std::move(schema_), ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DescTagValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus DescTagValidator::toPlan() {
    auto sentence = static_cast<DescribeTagSentence*>(sentence_);
    auto name = *sentence->name();
    auto doNode = DescTag::make(qctx_, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DescEdgeValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus DescEdgeValidator::toPlan() {
    auto sentence = static_cast<DescribeEdgeSentence*>(sentence_);
    auto name = *sentence->name();
    auto doNode = DescEdge::make(qctx_, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus AlterValidator::alterSchema(const std::vector<AlterSchemaOptItem*>& schemaOpts,
                                   const std::vector<SchemaPropItem*>& schemaProps) {
        for (auto& schemaOpt : schemaOpts) {
            meta::cpp2::AlterSchemaItem schemaItem;
            auto opType = schemaOpt->toType();
            schemaItem.set_op(opType);
            meta::cpp2::Schema schema;
            if (opType == meta::cpp2::AlterSchemaOp::DROP) {
                const auto& colNames = schemaOpt->columnNames();
                for (auto& colName : colNames) {
                    meta::cpp2::ColumnDef column;
                    column.name = *colName;
                    schema.columns.emplace_back(std::move(column));
                }
            } else {
                const auto& specs = schemaOpt->columnSpecs();
                for (auto& spec : specs) {
                    meta::cpp2::ColumnDef column;
                    column.name = *spec->name();
                    column.type = spec->type();
                    if (spec->hasDefaultValue()) {
                        column.set_default_value(spec->getDefaultValue());
                    }
                    if (spec->type() == meta::cpp2::PropertyType::FIXED_STRING) {
                        column.set_type_length(spec->typeLen());
                    }
                    if (spec->isNull()) {
                        column.set_nullable(true);
                    }
                    schema.columns.emplace_back(std::move(column));
                }
            }

            schemaItem.set_schema(std::move(schema));
            schemaItems_.emplace_back(std::move(schemaItem));
        }

        for (auto& schemaProp : schemaProps) {
            auto propType = schemaProp->getPropType();
            StatusOr<int64_t> retInt;
            StatusOr<std::string> retStr;
            int ttlDuration;
            switch (propType) {
                case SchemaPropItem::TTL_DURATION:
                    retInt = schemaProp->getTtlDuration();
                    if (!retInt.ok()) {
                        return GraphStatus::setInvalidParam("ttl_duration");
                    }
                    ttlDuration = retInt.value();
                    schemaProp_.set_ttl_duration(ttlDuration);
                    break;
                case SchemaPropItem::TTL_COL:
                    // Check the legality of the column in meta
                    retStr = schemaProp->getTtlCol();
                    if (!retStr.ok()) {
                        return GraphStatus::setInvalidParam("ttl_col");
                    }
                    schemaProp_.set_ttl_col(retStr.value());
                    break;
            }
        }
        return GraphStatus::OK();
}

GraphStatus AlterTagValidator::validateImpl() {
    auto sentence = static_cast<AlterTagSentence*>(sentence_);
    name_ = *sentence->name();
    return alterSchema(sentence->getSchemaOpts(), sentence->getSchemaProps());
}

GraphStatus AlterTagValidator::toPlan() {
    auto *doNode = AlterTag::make(qctx_,
                                  nullptr,
                                  vctx_->whichSpace().id,
                                  std::move(name_),
                                  std::move(schemaItems_),
                                  std::move(schemaProp_));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus AlterEdgeValidator::validateImpl() {
    auto sentence = static_cast<AlterEdgeSentence*>(sentence_);
    name_ = *sentence->name();
    return alterSchema(sentence->getSchemaOpts(), sentence->getSchemaProps());
}

GraphStatus AlterEdgeValidator::toPlan() {
    auto *doNode = AlterEdge::make(qctx_,
                                   nullptr,
                                   vctx_->whichSpace().id,
                                   std::move(name_),
                                   std::move(schemaItems_),
                                   std::move(schemaProp_));
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowTagsValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowTagsValidator::toPlan() {
    auto *doNode = ShowTags::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowEdgesValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowEdgesValidator::toPlan() {
    auto *doNode = ShowEdges::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowCreateTagValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowCreateTagValidator::toPlan() {
    auto sentence = static_cast<ShowCreateTagSentence*>(sentence_);
    auto *doNode = ShowCreateTag::make(qctx_,
                                       nullptr,
                                      *sentence->name());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus ShowCreateEdgeValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowCreateEdgeValidator::toPlan() {
    auto sentence = static_cast<ShowCreateEdgeSentence*>(sentence_);
    auto *doNode = ShowCreateEdge::make(qctx_,
                                        nullptr,
                                       *sentence->name());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DropTagValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus DropTagValidator::toPlan() {
    auto sentence = static_cast<DropTagSentence*>(sentence_);
    auto *doNode = DropTag::make(qctx_,
                                 nullptr,
                                *sentence->name(),
                                 sentence->isIfExists());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}

GraphStatus DropEdgeValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus DropEdgeValidator::toPlan() {
    auto sentence = static_cast<DropEdgeSentence*>(sentence_);
    auto *doNode = DropEdge::make(qctx_,
                                  nullptr,
                                 *sentence->name(),
                                  sentence->isIfExists());
    root_ = doNode;
    tail_ = root_;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
