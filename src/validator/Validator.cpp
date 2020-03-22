/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"
#include "parser/Sentence.h"
#include "planner/Query.h"
#include "validator/GoValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"

namespace nebula {
namespace graph {
std::unique_ptr<Validator> makeValidator(Sentence* sentence, ValidateContext* context) {
    CHECK(!!sentence);
    CHECK(!!context);
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kSequential:
            return std::make_unique<SequentialValidator>(sentence, context);
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence, context);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence, context);
        default:
            return std::make_unique<ReportError>(sentence, context);
    }
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    switch (node->kind()) {
        case PlanNode::Kind::kEnd: {
            // TODO:
            return Status::Error("Not implement.");
        }
        case PlanNode::Kind::kFilter: {
            static_cast<Filter*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kProject: {
            static_cast<Project*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kSort: {
            static_cast<Sort*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kLimit: {
            static_cast<Limit*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kAggregate: {
            static_cast<Aggregate*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kSelector: {
            static_cast<Selector*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kLoop: {
            static_cast<Loop*>(node)->setInput(appended);
            break;
        }
        case PlanNode::Kind::kRegisterSpaceToSession: {
            static_cast<RegisterSpaceToSession*>(node)->setInput(appended);
            break;
        }
        default: {
            return Status::Error(
                    "%ld not support to append an input.", static_cast<int64_t>(node->kind()));
        }
    }
    return Status::OK();
}

Status Validator::validate() {
    Status status;

    if (!validateContext_) {
        return Status::Error("Validate context was not given.");
    }

    if (!sentence_) {
        return Status::Error("Sentence was not given");
    }

    if (!spaceChosen()) {
        status = Status::Error("Space was not chosen.");
        return status;
    }

    status = validateImpl();
    if (!status.ok()) {
        return status;
    }

    status = toPlan();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return validateContext_->spaceChosen();
}
}  // namespace graph
}  // namespace nebula
