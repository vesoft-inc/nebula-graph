/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/SequentialPlanner.h"

#include "parser/Sentence.h"
#include "planner/Logic.h"
#include "planner/Query.h"
#include "validator/SequentialValidator.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {
std::unique_ptr<SequentialPlanner> SequentialPlanner::instance_ =
    std::unique_ptr<SequentialPlanner>(new SequentialPlanner());

SequentialPlanner::SequentialPlanner() {
    plannersMap()[Sentence::Kind::kSequential].emplace_back(this);
}

bool SequentialPlanner::match(Validator* validator) {
    if (validator->sentence()->kind() == Sentence::Kind::kSequential) {
        return true;
    } else {
        return false;
    }
}

StatusOr<SubPlan> SequentialPlanner::transform(Validator* validator) {
    SubPlan subPlan;
    auto* seqValidator = static_cast<SequentialValidator*>(validator);
    auto* qctx = seqValidator->qctx();
    const auto& validators = seqValidator->validators();
    subPlan.root = validators.back()->root();
    ifBuildDataCollect(subPlan, qctx);
    for (auto iter = validators.begin(); iter < validators.end() - 1; ++iter) {
        NG_RETURN_IF_ERROR((iter + 1)->get()->appendPlan(iter->get()->root()));
    }
    subPlan.tail = StartNode::make(qctx);
    NG_RETURN_IF_ERROR(validators.front()->appendPlan(subPlan.tail));
    VLOG(1) << "root: " << subPlan.root->kind() << " tail: " << subPlan.tail->kind();
    return subPlan;
}

void SequentialPlanner::ifBuildDataCollect(SubPlan& subPlan, QueryContext* qctx) {
    switch (subPlan.root->kind()) {
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kDedup:
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus: {
            auto* dc = DataCollect::make(qctx,
                                         subPlan.root,
                                         DataCollect::CollectKind::kRowBasedMove,
                                         {subPlan.root->outputVar()});
            dc->setColNames(subPlan.root->colNames());
            subPlan.root = dc;
            break;
        }
        default:
            break;
    }
}
}  // namespace graph
}  // namespace nebula
