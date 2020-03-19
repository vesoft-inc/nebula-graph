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

Status ExecutionContext::addVariable(const std::string& varName, cpp2::DataSet dataset) {
    UNUSED(varName);
    UNUSED(dataset);
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
