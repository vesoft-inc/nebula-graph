/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/SequentialValidator.h"
#include "service/GraphFlags.h"
#include "service/PermissionCheck.h"
#include "planner/Logic.h"
#include "planner/Query.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {
GraphStatus SequentialValidator::validateImpl() {
    GraphStatus status;
    if (sentence_->kind() != Sentence::Kind::kSequential) {
        return GraphStatus::setInternalError(
                folly::stringPrintf(
                "Sequential validator validates a SequentialSentences, but %ld is given.",
                static_cast<int64_t>(sentence_->kind())));
    }
    auto seqSentence = static_cast<SequentialSentences*>(sentence_);
    auto sentences = seqSentence->sentences();

    if (sentences.size() > static_cast<size_t>(FLAGS_max_allowed_statements)) {
        return GraphStatus::setOutOfMaxStatements();
    }

    DCHECK(!sentences.empty());
    auto firstSentence = getFirstSentence(sentences.front());
    switch (firstSentence->kind()) {
        case Sentence::Kind::kLimit:
        case Sentence::Kind::kOrderBy:
        case Sentence::Kind::kGroupBy:
            return GraphStatus::setSyntaxError(
                    folly::stringPrintf("Could not start with the statement: %s",
                    firstSentence->toString().c_str()));
        default:
            break;
    }

    GraphStatus gStatus;
    for (auto* sentence : sentences) {
        if (FLAGS_enable_authorize) {
            auto *session = qctx_->rctx()->session();
            /**
             * Skip special operations check at here. they are :
             * kUse, kDescribeSpace, kRevoke and kGrant.
             */
            if (!PermissionCheck::permissionCheck(DCHECK_NOTNULL(session), sentence)) {
                return GraphStatus::setPermissionDenied();
            }
        }
        auto validator = makeValidator(sentence, qctx_);
        gStatus = validator->validate();
        if (!gStatus.ok()) {
            return gStatus;
        }
        validators_.emplace_back(std::move(validator));
    }

    return GraphStatus::OK();
}

<<<<<<< HEAD
Status SequentialValidator::toPlan() {
=======
GraphStatus SequentialValidator::toPlan() {
    auto* plan = qctx_->plan();
>>>>>>> all use GraphStatus
    root_ = validators_.back()->root();
    ifBuildDataCollectForRoot(root_);
    GraphStatus gStatus;
    for (auto iter = validators_.begin(); iter < validators_.end() - 1; ++iter) {
        gStatus = (iter + 1)->get()->appendPlan(iter->get()->root());
        if (!gStatus.ok()) {
            return gStatus;
        }
    }
<<<<<<< HEAD
    tail_ = StartNode::make(qctx_);
    NG_RETURN_IF_ERROR(validators_.front()->appendPlan(tail_));
=======
    tail_ = StartNode::make(plan);
    gStatus = validators_.front()->appendPlan(tail_);
    if (!gStatus.ok()) {
        return gStatus;
    }
>>>>>>> all use GraphStatus
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return GraphStatus::OK();
}

const Sentence* SequentialValidator::getFirstSentence(const Sentence* sentence) const {
    if (sentence->kind() != Sentence::Kind::kPipe) {
        return sentence;
    }
    auto pipe = static_cast<const PipedSentence *>(sentence);
    return getFirstSentence(pipe->left());
}

void SequentialValidator::ifBuildDataCollectForRoot(PlanNode* root) {
    switch (root->kind()) {
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kDedup:
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus: {
            auto* dc = DataCollect::make(
                qctx_, root, DataCollect::CollectKind::kRowBasedMove, {root->varName()});
            dc->setColNames(root->colNames());
            root_ = dc;
            break;
        }
        default:
            break;
    }
}
}  // namespace graph
}  // namespace nebula
