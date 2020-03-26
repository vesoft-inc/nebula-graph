/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/ExecutionContext.h"

#include <folly/String.h>

#include "exec/Executor.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

nebula::cpp2::Value ExecutionContext::getValue(const std::string& varName) const {
    auto iter = variables_.find(varName);
    DCHECK(iter != variables_.end());
    return iter->second.back();
}

Status ExecutionContext::addValue(const std::string& varName, nebula::cpp2::Value value) {
    UNUSED(varName);
    UNUSED(value);
    return Status::OK();
}

Status ExecutionContext::checkValues() const {
    if (valueReservation_ < 0 || valueCounts_ < static_cast<uint64_t>(valueReservation_)) {
        return Status::OK();
    }

    return Status::Error(stringPrintf("Out of values limit: %ld", valueCounts_));
}

}   // namespace graph
}   // namespace nebula
