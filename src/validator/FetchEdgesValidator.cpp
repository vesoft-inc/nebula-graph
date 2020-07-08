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
                                  plan->root(),  // previous root as input
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
    if (withProject_) {
        auto *projectNode = Project::make(
            plan, current, sentence_->yieldClause()->yields());
        projectNode->setInputVar(current->varName());
        // TODO(shylock) waiting expression toString
        projectNode->setColNames(deduceColNames(projectNode->columns()));
        current = projectNode;
    }
    // Project select the properties then dedup
    if (dedup_) {
        auto *dedupNode = Dedup::make(plan, current);
        dedupNode->setInputVar(current->varName());
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result if required
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
        src_ = sentence_->ref()->moveSrcId();
        ranking_ = sentence_->ref()->moveRank();
        dst_ = sentence_->ref()->moveDstId();
        return Status::OK();
    }

    // from constant, eval now
    ExpressionContextImpl dummy = ExpressionContextImpl();
    auto keys = sentence_->keys();
    if (keys != nullptr) {
        // row: _src, _type, _ranking, _dst
        edges_.reserve(sentence_->keys()->keys().size());
        for (const auto &key : sentence_->keys()->keys()) {
            DCHECK(key->srcid()->isConstExpr());
            // TODO(shylock) Add new value type EDGE_ID to semantic and simplify this
            auto src = key->srcid()->eval(dummy);
            if (!src.isStr()) {   // string as vid
                return Status::NotSupported("src is not a vertex id");
            }
            auto ranking = key->rank();
            DCHECK(key->dstid()->isConstExpr());
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
    auto *yield = sentence_->yieldClause();
    storage::cpp2::EdgeProp prop;
    prop.set_type(edgeType_);
    // empty for all properties
    if (yield != nullptr) {
        std::vector<std::string> propsName;
        propsName.reserve(yield->columns().size());
        dedup_ = yield->isDistinct();
        exprs_.reserve(yield->columns().size());
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
                    if (*expr->sym() != edgeTypeName_) {
                        return Status::Error("Mismatched edge type name");
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
                exprs_.emplace_back(std::move(exprAlias));
            } else {
                // Need project to evaluate the expression not push down to storage
                // And combine the result from storage
                withProject_ = true;
            }
        }
        prop.set_props(std::move(propsName));
    }

    props_.emplace_back(std::move(prop));
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
