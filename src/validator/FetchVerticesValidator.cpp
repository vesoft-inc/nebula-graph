/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchVerticesValidator.h"
#include <memory>
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
    // Start [-> some input] -> GetVertices [-> Project] [-> Dedup] [-> next stage] -> End
    auto *plan = qctx_->plan();
    auto *doNode = GetVertices::make(plan,
                                     plan->root(),  // previous root as input
                                     spaceId_,
                                     std::move(vertices_),
                                     std::move(src_),
                                     std::move(props_),
                                     std::move(exprs_),
                                     dedup_,
                                     std::move(orderBy_),
                                     limit_,
                                     std::move(filter_));
    PlanNode *current = doNode;
    if (withProject_) {
        auto *projectNode = Project::make(
            plan, current, sentence_->yieldClause()->yields());
        projectNode->setInputVar(current->varName());
        // TODO(shylock) waiting expression toString
        projectNode->setColNames(deduceColNames(projectNode->columns()));
        current = projectNode;
    }
    // Project select properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current);
        current = dedupNode;
    }
    root_ = current;
    tail_ = doNode;
    return Status::OK();
}

Status FetchVerticesValidator::check() {
    spaceId_ = vctx_->whichSpace().id;

    if (sentence_->isAllTagProps()) {
        // empty for all tag
        tagName_ = *sentence_->tag();
    } else {
        tagName_ = *(sentence_->tag());
        auto tagStatus = qctx_->schemaMng()->toTagID(spaceId_, tagName_);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "No schema found for " << tagName_;
            return Status::Error("No schema found for `%s'", tagName_.c_str());
        }

        tagId_ = tagStatus.value();
        schema_ = qctx_->schemaMng()->getTagSchema(spaceId_, tagId_.value());
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
        src_ = sentence_->moveRef();
        return Status::OK();
    }

    // from constant, eval now
    // TODO(shylock) add eval() method for expression
    ExpressionContextImpl dummy = ExpressionContextImpl();
    auto vids = sentence_->vidList();
    vertices_.reserve(vids.size());
    for (const auto vid : vids) {
        // TODO(shylock) Add a new value type VID to semantic this
        DCHECK(vid->isConstExpr());
        auto v = vid->eval(dummy);
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
        // empty for all tag and properties
        props_.clear();
        if (!sentence_->isAllTagProps()) {
            // for one tag all properties
            storage::cpp2::VertexProp prop;
            prop.set_tag(tagId_.value());
            // empty for all
            props_.emplace_back(std::move(prop));
        }
    } else {
        CHECK(!sentence_->isAllTagProps()) << "Not supported yield for *.";
        dedup_ = yield->isDistinct();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId_.value());
        std::vector<std::string> propsName;
        propsName.reserve(yield->columns().size());
        for (const auto col : yield->columns()) {
            // The properties from storage directly push down only
            // The other will be computed in Project Executor
            const auto storageExprs = col->expr()->findAllStorage();
            if (!storageExprs.empty()) {
                if (storageExprs.size() == 1 && col->expr()->isStorage()) {
                    // only one expression it's storage property expression
                } else {
                    // need computation in project when storage not do it.
                    withProject_ = true;
                }
                for (const auto &storageExpr : storageExprs) {
                    const auto *expr = static_cast<const SymbolPropertyExpression *>(storageExpr);
                    if (*expr->sym() != tagName_) {
                        return Status::Error("Mismatched tag name");
                    }
                    // Check is prop name in schema
                    if (schema_->getFieldIndex(*expr->prop()) < 0) {
                        LOG(ERROR) << "Unknown column `" << *expr->prop() << "' in schema";
                        return Status::Error("Unknown column `%s' in schema",
                                                expr->prop()->c_str());
                    }
                    propsName.emplace_back(*expr->prop());
                }
                storage::cpp2::Expr exprAlias;
                if (col->alias()) {
                    exprAlias.set_alias(*col->alias());
                }
                exprAlias.set_expr(col->expr()->encode());
                exprs_.emplace_back(exprAlias);
            } else {
                // Need project to evaluate the expression not push down to storage
                // And combine the result from storage
                withProject_ = true;
            }
        }
        prop.set_props(std::move(propsName));
        props_.emplace_back(std::move(prop));
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
