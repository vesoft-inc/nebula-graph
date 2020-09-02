/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"

#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

GraphStatus SetValidator::validateImpl() {
    auto setSentence = static_cast<SetSentence *>(sentence_);
    lValidator_ = makeValidator(setSentence->left(), qctx_);
    auto gStatus = lValidator_->validate();
    if (!gStatus.ok()) {
        return gStatus;
    }
    rValidator_ = makeValidator(setSentence->right(), qctx_);
    gStatus = rValidator_->validate();
    if (!gStatus.ok()) {
        return gStatus;
    }

    auto lCols = lValidator_->outputCols();
    auto rCols = rValidator_->outputCols();

    if (lCols.size() != rCols.size()) {
        return GraphStatus::setSemanticError(
                "number of columns to UNION/INTERSECT/MINUS must be same");
    }

    for (size_t i = 0, e = lCols.size(); i < e; i++) {
        if (lCols[i].first != rCols[i].first) {
            return GraphStatus::setSemanticError(
                    "different column names to UNION/INTERSECT/MINUS are not supported");
        }
    }

    outputs_ = std::move(lCols);
    return GraphStatus::OK();
}

GraphStatus SetValidator::toPlan() {
    auto setSentence = static_cast<const SetSentence *>(sentence_);
    auto lRoot = DCHECK_NOTNULL(lValidator_->root());
    auto rRoot = DCHECK_NOTNULL(rValidator_->root());
    auto colNames = lRoot->colNames();
    BiInputNode *bNode = nullptr;
    switch (setSentence->op()) {
        case SetSentence::Operator::UNION: {
            bNode = Union::make(qctx_, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            if (setSentence->distinct()) {
                auto dedup = Dedup::make(qctx_, bNode);
                dedup->setInputVar(bNode->varName());
                dedup->setColNames(bNode->colNames());
                root_ = dedup;
            } else {
                root_ = bNode;
            }
            break;
        }
        case SetSentence::Operator::INTERSECT: {
            bNode = Intersect::make(qctx_, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            root_ = bNode;
            break;
        }
        case SetSentence::Operator::MINUS: {
            bNode = Minus::make(qctx_, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            root_ = bNode;
            break;
        }
        default:
            return GraphStatus::setInternalError(
                    folly::stringPrintf("Unknown operator: %ld",
                            static_cast<int64_t>(setSentence->op())));
    }

    bNode->setLeftVar(lRoot->varName());
    bNode->setRightVar(rRoot->varName());

    tail_ = PassThroughNode::make(qctx_, nullptr);
    auto gStatus = lValidator_->appendPlan(tail_);
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = rValidator_->appendPlan(tail_);
    if (!gStatus.ok()) {
        return gStatus;
    }
    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula

