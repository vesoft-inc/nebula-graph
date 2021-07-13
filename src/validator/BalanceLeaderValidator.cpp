/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"

#include "planner/plan/Admin.h"
#include "validator/BalanceLeaderValidator.h"

namespace nebula {
namespace graph {

Status BalanceLeaderValidator::toPlan() {
    auto *doNode = BalanceLeader::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
