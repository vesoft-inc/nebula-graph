/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchEdgesValidator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status FetchEdgesValidator::validateImpl() {
    auto status = Status::OK();
    if (!spaceChosen()) {
        return Status::Error("Please choose a graph space with `USE spaceName' firstly");
    }

    status = check();
    if (!status.ok()) {
        return status;
    }

    status = prepareEdges();
    if (!status.ok()) {
        return status;
    }

    status = prepareProperties();
    if (!status.ok()) {
        return status;
    }
    return status;
}

Status FetchEdgesValidator::toPlan() {
    // Start [-> some input] -> GetEdges -> Project -> End
    auto *plan = validateContext_->plan();
    auto *doNode = GetEdges::make(plan,
                                  nullptr /*TODO(shylock) INPUT*/,
                                  spaceId_,
                                  std::move(edges_),
                                  src_,
                                  edgeType_,
                                  ranking_,
                                  dst_,
                                  std::move(props_),
                                  dedup_,
                                  limit_,
                                  std::move(orderBy_),
                                  std::move(filter_));
    auto *projectNode = Project::make(plan, doNode, sentence_->yieldClause()->yieldColumns());
    root_ = projectNode;
    tail_ = root_;
    return Status::OK();
}

Status FetchEdgesValidator::check() {
    spaceId_ = validateContext_->whichSpace().id;
    edgeTypeName_ = *sentence_->edge();
    auto edgeStatus = validateContext_->schemaMng()->toEdgeType(spaceId_, edgeTypeName_);
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    edgeType_ = edgeStatus.value();
    schema_ = validateContext_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence_->edge();
        return Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
    }

    return Status::OK();
}

Status FetchEdgesValidator::prepareEdges() {
    if (sentence_->isRef()) {
        src_ = sentence_->ref()->srcid();
        ranking_ = sentence_->ref()->rank();
        dst_ = sentence_->ref()->dstid();
    }

    auto keys = sentence_->keys();
    if (keys != nullptr) {
        // row: _src, _type, _ranking, _dst
        edges_.reserve(sentence_->keys()->keys().size());
        for (const auto &key : sentence_->keys()->keys()) {
            edges_.emplace_back(nebula::Row({key->srcid(), edgeType_, key->rank(), key->dstid()}));
        }
    }
    return Status::OK();
}

Status FetchEdgesValidator::prepareProperties() {
    auto *yield = sentence_->yieldClause();
    if (yield == nullptr) {
        // empty for all properties
        props_.clear();
    } else {
        dedup_ = yield->isDistinct();
        for (const auto col : yield->columns()) {
            if (col->expr()->isAliasPropertyExpression()) {
                auto *expr = static_cast<AliasPropertyExpression *>(col->expr());
                if (*expr->alias() != edgeTypeName_) {
                    return Status::Error("Mismatched edge type name");
                }
                // Check is prop name in schema
                if (schema_->getFieldIndex(*expr->prop()) < 0) {
                    LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in schema";
                    return Status::Error("Unknown column `%s' in schema", expr->prop()->c_str());
                }
                props_.emplace_back(expr->encode());
            } else {
                return Status::NotSupported("Unsupported expression");
            }
        }
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
