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
Status CreateTagValidator::validateImpl() {
    auto status = Status::OK();
    tagName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(tagName_);
        if (pro != nullptr) {
            status = Status::Error("Has the same name `%s' in the SequentialSentences",
                                    tagName_.c_str());
            break;
        }

        status = SchemaUtil::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);
    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(tagName_, schemaPro);
    }
    return status;
}

Status CreateTagValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = CreateTag::make(plan,
            plan->root(), std::move(tagName_), std::move(schema_), ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status CreateEdgeValidator::validateImpl() {
    auto status = Status::OK();
    edgeName_ = *sentence_->name();
    ifNotExist_ = sentence_->isIfNotExist();
    do {
        // Check the validateContext has the same name schema
        auto pro = vctx_->getSchema(edgeName_);
        if (pro != nullptr) {
            status = Status::Error("Has the same name `%s' in the SequentialSentences",
                                    edgeName_.c_str());
            break;
        }

        status = SchemaUtil::validateColumns(sentence_->columnSpecs(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
        status = SchemaUtil::validateProps(sentence_->getSchemaProps(), schema_);
        if (!status.ok()) {
            VLOG(1) << status;
            break;
        }
    } while (false);

    // Save the schema in validateContext
    if (status.ok()) {
        auto schemaPro = SchemaUtil::generateSchemaProvider(0, schema_);
        vctx_->addSchema(edgeName_, schemaPro);
    }
    return status;
}

Status CreateEdgeValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto doNode = CreateEdge::make(plan,
            plan->root(), std::move(edgeName_), std::move(schema_), ifNotExist_);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescTagValidator::validateImpl() {
    return Status::OK();
}

Status DescTagValidator::toPlan() {
    auto sentence = static_cast<DescribeTagSentence*>(sentence_);
    auto name = *sentence->name();
    auto *plan = qctx_->plan();
    auto doNode = DescTag::make(plan, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status DescEdgeValidator::validateImpl() {
    return Status::OK();
}

Status DescEdgeValidator::toPlan() {
    auto sentence = static_cast<DescribeEdgeSentence*>(sentence_);
    auto name = *sentence->name();
    auto *plan = qctx_->plan();
    auto doNode = DescEdge::make(plan, nullptr, std::move(name));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status AlterValidator::alterSchema(const std::vector<AlterSchemaOptItem*>& schemaOpts,
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
                        return retInt.status();
                    }
                    ttlDuration = retInt.value();
                    schemaProp_.set_ttl_duration(ttlDuration);
                    break;
                case SchemaPropItem::TTL_COL:
                    // Check the legality of the column in meta
                    retStr = schemaProp->getTtlCol();
                    if (!retStr.ok()) {
                        return retStr.status();
                    }
                    schemaProp_.set_ttl_col(retStr.value());
                    break;
                default:
                    return Status::Error("Property type not support");
            }
        }
        return Status::OK();
}

Status AlterTagValidator::validateImpl() {
    auto sentence = static_cast<AlterTagSentence*>(sentence_);
    name_ = *sentence->name();
    return alterSchema(sentence->getSchemaOpts(), sentence->getSchemaProps());
}

Status AlterTagValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = AlterTag::make(plan,
                                  vctx_->whichSpace().id,
                                  std::move(name_),
                                  std::move(schemaItems_),
                                  std::move(schemaProp_));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status AlterEdgeValidator::validateImpl() {
    auto sentence = static_cast<AlterEdgeSentence*>(sentence_);
    name_ = *sentence->name();
    return alterSchema(sentence->getSchemaOpts(), sentence->getSchemaProps());
}

Status AlterEdgeValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = AlterEdge::make(plan,
                                   vctx_->whichSpace().id,
                                   std::move(name_),
                                   std::move(schemaItems_),
                                   std::move(schemaProp_));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
