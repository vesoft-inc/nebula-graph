/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchVerticesValidator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status FetchVerticesValidator::validateImpl() {
    auto status = Status::OK();
    if (!spaceChosen()) {
        return Status::Error("Please choose a graph space with `USE spaceName' firstly");
    }

    status = check();
    if (!status.ok()) {
        return status;
    }

    status = prepareVertices();
    if (!status.ok()) {
        return status;
    }

    status = prepareProperties();
    if (!status.ok()) {
        return status;
    }
    return status;
}

Status FetchVerticesValidator::toPlan() {
    // Start [-> some input] -> GetVertices [->DeDup] -> Project [-> next stage] -> End
    auto *plan = validateContext_->plan();
    auto *doNode = GetVertices::make(plan,
                                     root_,  // previous root as input
                                     spaceId_,
                                     std::move(vertices_),
                                     src_,
                                     std::move(props_),
                                     dedup_,
                                     std::move(orderBy_),
                                     limit_,
                                     std::move(filter_));
    auto *current = doNode;
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current, nullptr);
        current = dedupNode;
    }
    auto *projectNode = Project::make(plan, current, sentence_->yieldClause()->yieldColumns());
    root_ = projectNode;
    tail_ = doNode;
    return Status::OK();
}

Status FetchVerticesValidator::check() {
    spaceId_ = validateContext_->whichSpace().id;

    if (sentence_->isAllTagProps()) {
        // empty for all tag
        tagName_ = *sentence_->tag();
    } else {
        tagName_ = *(sentence_->tag());
        auto tagStatus = validateContext_->schemaMng()->toTagID(spaceId_, tagName_);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "No schema found for " << tagName_;
            return Status::Error("No schema found for `%s'", tagName_.c_str());
        }

        tagId_ = tagStatus.value();
        schema_ = validateContext_->schemaMng()->getTagSchema(spaceId_, tagId_.value());
        if (schema_ == nullptr) {
            LOG(ERROR) << "No schema found for " << tagName_;
            return Status::Error("No schema found for `%s'", tagName_.c_str());
        }
    }
    return Status::OK();
}

Status FetchVerticesValidator::prepareVertices() {
    // from ref, eval when execute
    if (sentence_->isRef()) {
        src_ = sentence_->ref();
        return Status::OK();
    }

    // from constant, eval now
    auto vids = sentence_->vidList();
    vertices_.reserve(vids.size());
    for (const auto vid : vids) {
        // TODO(shylock) Add a new value type VID to semantic this
        auto v = vid->eval();
        if (!v.isStr()) {   // string as vid
            return Status::NotSupported("Not a vertex id");
        }
        vertices_.emplace_back(nebula::Row({std::move(v).getStr()}));
    }
    return Status::OK();
}

Status FetchVerticesValidator::prepareProperties() {
    auto *yield = sentence_->yieldClause();
    if (yield == nullptr) {
        // empty for all properties
        props_.clear();
    } else {
        dedup_ = yield->isDistinct();
        for (const auto col : yield->columns()) {
            if (col->expr()->isAliasPropertyExpression()) {
                if (tagId_.hasValue()) {   // check properties when specified TAG
                    auto *expr = static_cast<AliasPropertyExpression *>(col->expr());
                    if (*expr->alias() != tagName_) {
                        return Status::Error("Mismatched tag name");
                    }
                    // Check is prop name in schema
                    if (schema_->getFieldIndex(*expr->prop()) < 0) {
                        LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in schema";
                        return Status::Error("Unknown column `%s' in schema",
                                             expr->prop()->c_str());
                    }
                }
                props_.emplace_back(col->expr()->encode());
            } else {
                return Status::NotSupported("Unsupported expression");
            }
        }
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
