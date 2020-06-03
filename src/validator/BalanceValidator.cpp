/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"

#include "planner/Admin.h"
#include "validator/BalanceValidator.h"

namespace nebula {
namespace graph {

Status BalanceValidator::toPlan() {
    auto* plan = validateContext_->plan();
    PlanNode *current = nullptr;
    switch (sentence_->subType()) {
    case BalanceSentence::SubType::kLeader:
        current = BalanceLeaders::make(plan);
        break;
    case BalanceSentence::SubType::kData:
        current = Balance::make(plan,
                                sentence_->hostDel() == nullptr
                                    ? std::vector<HostAddr>()
                                    : sentence_->hostDel()->hosts());
        break;
    case BalanceSentence::SubType::kDataStop:
        current = StopBalance::make(plan);
        break;
    case BalanceSentence::SubType::kShowBalancePlan:
        current = ShowBalance::make(plan, sentence_->balanceId());
        break;
    case BalanceSentence::SubType::kUnknown:
        // fallthrough
    default:
        return Status::NotSupported("Unknown balance kind %d", sentence_->kind());
    }
    root_ = current;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
