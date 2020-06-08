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
    // Start [-> some input] -> GetEdges [-> Project] [-> Dedup] [-> next stage] -> End
    auto *plan = qctx_->plan();
    auto *doNode = GetEdges::make(plan,
                                  root_,  // previous root as input
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
    PlanNode *current = doNode;
    if (sentence_->yieldClause() != nullptr) {
        auto *projectNode = Project::make(plan, current, sentence_->yieldClause()->yieldColumns());
        current = projectNode;
    }
    // Project select the properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current, nullptr);
        current = dedupNode;
    }
    root_ = current;
    tail_ = doNode;
    return Status::OK();
}

Status FetchEdgesValidator::check() {
    spaceId_ = vctx_->whichSpace().id;
    edgeTypeName_ = *sentence_->edge();
    auto edgeStatus = qctx_->schemaMng()->toEdgeType(spaceId_, edgeTypeName_);
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    edgeType_ = edgeStatus.value();
    schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence_->edge();
        return Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
    }

    return Status::OK();
}

Status FetchEdgesValidator::prepareEdges() {
    // from ref, eval in execute
    if (sentence_->isRef()) {
        src_ = sentence_->ref()->srcid();
        ranking_ = sentence_->ref()->rank();
        dst_ = sentence_->ref()->dstid();
        return Status::OK();
    }

    // from constant, eval now
    std::unique_ptr<ExpressionContext> dummy = std::make_unique<ExpressionContextImpl>();
    auto keys = sentence_->keys();
    if (keys != nullptr) {
        // row: _src, _type, _ranking, _dst
        edges_.reserve(sentence_->keys()->keys().size());
        for (const auto &key : sentence_->keys()->keys()) {
            // TODO(shylock) Add new value type EDGE_ID to semantic and simplify this
            auto src = key->srcid()->eval(*dummy);
            if (!src.isStr()) {   // string as vid
                return Status::NotSupported("src is not a vertex id");
            }
            auto ranking = key->rank();
            auto dst = key->dstid()->eval(*dummy);
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
    auto *yield = sentence_->yieldClause();
    if (yield == nullptr) {
        // "edge.*"
        props_.clear();
        EdgePropertyExpression expr(new std::string(*sentence_->edge()),
                                    new std::string("*"));
        storage::cpp2::PropExp p;
        p.set_alias(""/*TODO(shylock) Maybe extra*/);
        p.set_prop(expr.encode());
        props_.emplace_back(std::move(p));
    } else {
        dedup_ = yield->isDistinct();
        for (const auto col : yield->columns()) {
            if (col->expr()->kind() == Expression::Kind::kEdgeProperty ||
                col->expr()->kind() == Expression::Kind::kDstProperty ||
                col->expr()->kind() == Expression::Kind::kSrcProperty ||
                col->expr()->kind() == Expression::Kind::kEdgeSrc ||
                col->expr()->kind() == Expression::Kind::kEdgeType ||
                col->expr()->kind() == Expression::Kind::kEdgeRank ||
                col->expr()->kind() == Expression::Kind::kEdgeDst) {
                auto *expr = static_cast<SymbolPropertyExpression *>(col->expr());
                if (*expr->sym() != edgeTypeName_) {
                    return Status::Error("Mismatched edge type name");
                }
                // Check is prop name in schema
                if (schema_->getFieldIndex(*expr->prop()) < 0) {
                    LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in schema";
                    return Status::Error("Unknown column `%s' in schema", expr->prop()->c_str());
                }
                storage::cpp2::PropExp p;
                p.set_alias(""/*TODO(shylock) Maybe extra*/);
                p.set_prop(col->expr()->encode());
                props_.emplace_back(std::move(p));
            } else {
                LOG(ERROR) << "Unsupported expression " << col->expr()->kind();
                return Status::NotSupported("Unsupported expression %d",
                                            static_cast<int>(col->expr()->kind()));
            }
        }
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
